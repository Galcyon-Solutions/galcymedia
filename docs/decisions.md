# Design decisions

The long whys that do not fit in a code comment: why the library does things
one way and not another. Each section exists because someone may want to
change the decision, and before doing so it helps to know what motivated it.
Only the decisions an integrator could take the other way from the outside
are here, or the ones that explain something they see.

This is the *why of our choices*. For the *how the Asterisk channel behaves*
(the commands, the events, the limits, the protocol gotchas), see
[protocol.md](protocol.md).

## Barge-in looks at whether the bot is playing, not whether the provider is generating

A voice engine generates ten seconds of speech in one or two and queues them
at once (that is where the XOFF comes from): when the provider announces the
end of the turn, that audio is still playing in Asterisk's queue for several
seconds. A barge-in that only looks at whether the provider is generating
(`_speaking`, which goes off in `end_turn`) flushes nothing in that window,
with the provider done and the audio still playing, and the bot does not go
quiet until the queue runs out. It is the common case as soon as the person
interrupts a long sentence: "the bot is talking" has two meanings, and
confusing them costs the barge-in.

So `interrupt()` flushes if there is audio in play: the provider generating
(`_speaking`) or its audio still playing (`_audio_pending`). `_audio_pending`
goes on when each frame is sent and goes off only when Asterisk confirms the
queue empty (`QUEUE_DRAINED`, which `end_turn` requests with
`REPORT_QUEUE_DRAINED`). That preserves the other case: with the bot truly
quiet, nothing generated and the queue empty, there is nothing to flush, and
doing it would take the freshly created reply with it. The "truly quiet"
signal is the channel's, not an estimate of ours: Asterisk tells the truth
about its own queue. Pinned by
`test_bot_audio_playing_stays_after_end_turn_until_drained`.

## `finish()` waits for the queue-drained notice, not a mark

The obvious way to hang up when the audio ends is the channel's mechanism
for that: `MARK_MEDIA`, which queues a marker and reports when it has
played. We do something else, for two measured reasons.

The first: the common case is that the bot says goodbye, the person
interrupts it (flush) and the model asks to hang up. `FLUSH_MEDIA` frees
every frame in the queue without looking at its type (Asterisk 23.4.1,
`chan_websocket.c:811-813`), so a mark in flight is destroyed, its ack never
arrives and the goodbye sits in silence until the timeout runs out. The
queue-drained notice (`REPORT_QUEUE_DRAINED`) is a channel flag that
survives the flush, so it hangs up at once. (The exact difference between
the two mechanisms is in protocol.md, section "Marks vs the queue-drained
notice".)

The second: the notice alone is not enough. When the model asks to hang up
through a tool, its goodbye does not exist yet: it takes about a second to
generate it. At that instant nothing is playing and the queue is empty, so
requesting the notice there waits for the drain of the previous turn, which
comes right away, and the call hangs up just as the goodbye was starting to
play. Measured: the model asked to hang up at 17:53:39 and its goodbye
started at 17:53:40.

So `finish()` waits for two things in order: first for the bot to have and
release its turn, and then for the queue-drained notice. With a short
breather at the start in case the goodbye is about to begin, and without
hanging forever if it never comes, because hanging up without a goodbye is
legitimate. The details of that wait (`speech.turn`, `finish_max_wait_s`)
are in architecture.md, "How a call starts and how it ends".

That this wait lives here and not in every application is the underlying
decision: knowing whether any bot audio is still in play requires speaking
the channel's protocol and tracking the turn. Moving it outside would force
every integrator to reimplement it with their provider's events, which
differ across the three. The integrator decides when the call ends; the
library makes sure it does not get cut mid-sentence.

## The XOFF wait is bounded

Two commands stop the channel clock that emits the XON, and with either one
an active XOFF is never lifted (see protocol.md, section "Flow control:
XOFF / XON"). Without a limit, waiting for the XON would block the task
forever. Dropping the frame after 5 seconds (`xoff_max_wait_s`) is the right
thing in real-time audio: arriving late is worse than not arriving, and
Asterisk's queue already holds about 20 seconds, so at 5 without movement
the clock is not running.

## The loop that reads the socket never waits on the provider

What the channel's official example does (`asterisk-websocket-examples`,
commit `e50f467`, `ast_media_websocket.py:78-107`) is handle everything
inside the same `async for` that reads the socket: right there it does
`await ws_media.send(...)` and `await lock.acquire()` for the XOFF. It is the
obvious path, and the one a hand-written adapter repeats: the provider's
`start()` and `send_audio()` awaited inside the reader loop.

We do something else: the reader loop never awaits anything of the
provider's that can hang. `start()` runs in a separate task, and the
caller's audio goes through a bounded queue (`AUDIO_IN_MAX_FRAMES`, 50
frames) that another task consumes and that drops the oldest frame when
full, which in real-time audio is the least useful one: the same decoupling
Asterisk does on the outbound side with its queue and the XOFF/XON.
`close()` is bounded with a timeout as well.

It matters because a single process serves every call, and it is the reader
loop that detects the caller hung up. Measured with the provider awaited
inside the loop: a hung `start()` (slow DNS, a socket to the STT that does
not answer) delays the server shutdown by 15 seconds, and a hung
`send_audio()` (a deadlock in the adapter) blocks it indefinitely, because
`serve()` waits for the handler to finish and the handler is stuck in
provider code. With the separate task, a hung provider jams only its own
task; the loop keeps reading, sees the close at once and cleans up.

## The synthetic HANGUP

The channel sends no hangup event (Asterisk 23.4.1, the nine
`_create_event_*` of `chan_websocket.c:198-430`): the only thing that
announces the end is the WebSocket close code (see protocol.md, section
"Events"). The obvious thing is to leave that end on a different path from
the rest, and that forces every integrator to handle two paths. We do the
opposite: the library translates the close into a synthetic HANGUP event,
which arrives on the same path as everything else and carries `channel_id`
like the real events, so a hook shared between concurrent calls can tell
them apart.

## The rate table is explicit, not derived from the name

`slin44` is 44100 Hz (`main/codec_builtin.c:371`), the only `slin` whose
name is not the rate in kHz. Deriving the rate from the name, the number
times a thousand, works on the other eight and fails right there, and raises
nothing: a serializer that does `int(format[4:]) * 1000` delivers 44000 and
the voice comes out at another speed. That is why `_SAMPLE_RATE_BY_FORMAT`
lists each format with its number, taken from the source, and
`test_slin44_is_44100_hz_and_not_44000` pins it.

## `parse_event` does not search substrings

The official examples do `if "MEDIA_START" in raw`, and it is a latent bug:
the content of MEDIA_START includes every channel variable, so a variable
whose value contains the name of another event would take the wrong branch.
The JSON is parsed and the `event` field is read.

## The default provider is the echo

A real provider as the default looks more useful and is worse: an extension
that forgot `Set(_AI_PROVIDER=...)` dies with an authentication error, and
the message blames a missing key when what was missing was a dialplan line.
The echo needs no keys and no network, so that extension answers something
and the problem shows where it is.

## The user picks the codec, the library adapts

The obvious thing is to fix a codec (`c(ulaw)` always) or to add an AGI that
inspects the trunk and picks it for us. Both options tie the user down: the
first denies them wideband audio when their trunk does have it; the second
adds an extra ceremony to the dialplan that someone who does not know much
will not understand.

Instead, the library reads the `format` from the `MEDIA_START` and adapts:
it derives from it the frame size for alignment and the silence byte (0x00
in linear PCM, 0xD5 in alaw, 0xFF in ulaw), without the adapter having to
know. The user picks the codec in the `Dial` for their case, which we
document with its trade-offs (the README table): `c(ulaw)` for a provider
that speaks telephony, `c(slin16)` for one that works in PCM. The link that
sets the quality is the trunk, not the WebSocket, so asking for 16 kHz over
an 8 kHz trunk only interpolates band that does not exist.

And faced with the seven codecs that put the channel in passthrough mode
(opus, g729, speex and company: the list with their `minimum_bytes` is in
protocol.md, section "The codecs and passthrough mode"; you lose barge-in,
the marks and the clean hangup, and the audio is compressed), the library
warns loudly on the way out, instead of letting the call start silently
toward the problem. The stance is deliberate: we would rather the user
improve their system than degrade ours to accommodate a bad codec.

## FastAGI and not func_curl, AMI or ARI

The way back to the dialplan has to do one thing: after the `Dial`, leave
the bot's decision (hang up, transfer, where to) written on the channel for
the dialplan to read. `func_curl`, AMI and ARI are Asterisk's known paths;
we do FastAGI, for what each one can do at exactly that point. Citations are
from Asterisk 23.4.1.

FastAGI writes on the channel. `AGI(agi://host:4573/after-dial,${CALL_ID})`
in the priority after the `Dial` (the examples' `extensions.conf`) opens a socket
to the application, and the application answers with commands from the
table in `res_agi.c:3879-3914`: `SET VARIABLE` (`:3910`), `GET VARIABLE`
(`:3889`), `EXEC` (`:3885`), `HANGUP` (`:3890`). That is what `decisions.py`
does: three `set_variable` with the action, the reason and the destination
(`agi.py` sends the `SET VARIABLE`). And it holds even if the caller already
hung up: the commands flagged "dead-safe" in that table are still accepted
in dead mode (`res_agi.c:4737`). The whole protocol is text over TCP and
fits in forty lines with no dependencies.

Two AGIs, one variable space. The AGI after the `Dial` and the one of the
hangup handler both run on the caller's channel, so the second writer wins
in the CDR. `Transfers` therefore answers the default (`BOT_ACTION=hangup`)
on the FIRST ask for a call only and remembers what it applied, for the
same TTL as a pending decision: a later ask for the same key, through
`handler` or `closing_handler`, leaves the channel as it is. Without that,
one handler mounted on every AGI path (`AgiServer(transfers.handler)`, the
shape a Pipecat pipeline takes when it serves the AGI itself) wrote `hangup`
over a transfer the dialplan had already applied, and the CDR of a
transferred call read `hangup` (real call, 2026-08-22;
`test_each_decision_is_delivered_once`).

`func_curl` only asks. `CURL()` is a dialplan function (`func_curl.c:1004-1008`,
`acf_curl` with `read2` and `write`): the dialplan evaluates it when it
reaches that line and it returns a string. Nothing on the other side
initiates toward the dialplan, and the answer has to be ready at the instant
the dialplan asks.

AMI is a global stream per connection, not per call. Every event enters a
single queue (`all_events`, `manager.c:159`) and each session receives from
it what its `read=` permission class admits (`readperm & category`,
`manager.c:6445-6446`), with `eventfilter` to trim by name or header
(`:5714-5717`). There is no per-channel subscription: a bot that wants its
call filters the whole switch. And the cost does not depend on the
permission: with any AMI session connected, every RTCP packet of every call
becomes event text and enters that global queue (`manager_default_msg_cb`,
`manager.c:568-587`, which only asks whether anyone is connected;
`append_event` without looking at permissions, `:7630`); `read=` filters
only on delivery. Before AMI the message already exists: `rtp_engine.c:3777`
publishes it on the `rtp:all` topic (`:3900`), which one thread per
subscriber consumes (`stasis.c:977`). Measured in production (Asterisk
23.2.0, 2026-07-23, peaks of 70+ calls): the `stasis/m:rtp:all`
taskprocessor with Max Depth 6412 and 211 s of wait, and PJSIP answering
503. Turning it off is `decline=ast_rtp_rtcp_sent_type` and
`ast_rtp_rtcp_received_type` in `stasis.conf` (`stasis.conf.sample:12-21`),
which requires a restart and leaves Homer without RTCP.

ARI requires `Stasis()`. Any operation on a channel answers
`409 Channel not in Stasis application` if the channel is not inside a
Stasis application (`res/ari/resource_channels.c:161-173`,
`res_ari_channels.c:775`). That is, `Stasis()` instead of `Dial()`: call
control moves to the application, which is a whole architecture change. As
context, not as argument: ARI's audio path is `POST /channels/externalMedia`
(`rest-api/api-docs/channels.json:2006`), which by default delivers RTP over
UDP (`encapsulation` `rtp`, `transport` `udp`, `:2053-2076`), so the
application has to speak RTP, which is a different problem from speaking a
WebSocket. ARI has no
RTCP events in 23.4.1: only the manager (`manager.c:9597`) and
`res_hep_rtcp.c:165` subscribe to `rtp:all`, so the topic's cost exists all
the same, but it does not reach the application.

During development `func_curl` also failed after the `Dial`, and that was
taken as the reason. It is not: it was Asterisk issue 2038, fixed in
23.5.0-rc1 (see protocol.md, section "The "dangerous" dialplan functions do
not work after the `Dial`"). The decision stands without it, for the above.

## `serve()` is a class you can await or embed

The obvious thing, and what an integrator builds first, is a function that
runs forever (`await serve(...)`) plus a stop future to shut it down. It
covers the dedicated process, but not the one who wants the media server
living alongside the rest of their application (a panel, another server,
their own tasks), and the future is a home-grown mechanism one has to know.

We do what `websockets` does with its chameleon class: awaitable and async
context manager on the same object, with `__aenter__` that starts and
`__aexit__` that closes in order. `await serve(...)` runs forever;
`async with serve(...) as server:` embeds it, and `server.close()` works
from any task, with no new dependency.

## Policies are kwargs; the channel's limits are not

Every value that governs operational behavior and is a policy of ours (the
XON and drain waits, the provider timeouts, the inbound queue cap, the
observer caps, the AGI timeouts) is exposed as a keyword-only kwarg with its
usual default: a fork for lack of a parameter is a design failure of ours,
not the user's. The pattern is the one from `websockets` (flat kwargs with
defaults in plain sight), not a config object: our surface is an entry point
with independent scalars, which is exactly the case where aiohttp and httpx
do not use an object either. The channel's limits (128 control bytes, 65535
per message, of which the library stops at 65500, the 900/800/1000 queue
thresholds) are not exposed: they are the contract with Asterisk, and
"configuring" them would only manufacture incompatibility.

## No provider appears as a concrete type in the core

`ProviderRegistry` stores factories and `Session` receives a
`VoiceProvider`, which is a `Protocol`. Nowhere in the library is Deepgram,
OpenAI or ElevenLabs named as a type: if that happened, the provider would
be part of the architecture and the argument that the agent is
interchangeable would collapse on its own.

The contrast is in the voice bridge the `asterisk` org itself publishes
(`asteriskvoicebridge`, commit `e3eb810`, 2025-08-25). Its central struct
declares, in `voicebot/voicebot.go:365`:

```go
ttsprovider *deepgram.TTSDeepgram
sttprovider *deepgram.DeepgramSTTProvider
```

With those two lines, changing providers stops being writing a file and
becomes touching the core.

And what that repository says about itself has to be said, because it
changes what can be concluded: its README opens with *"This repository is
for demonstration purposes only and should not be used for production under
any circumstances"*. So it is not their product, it is an example, and an
example uses concrete types to be shorter to read. The lesson is not about
them: it is about what that shape costs in a library that does intend to
last.

## The adapters live inside, but install separately

The core does not know any voice provider exists, and that ignorance is
what keeps it alive when the AI market shifts. But whoever arrives does not
want to write an adapter from scratch: they want their Asterisk talking to
Pipecat today.

Both hold with optional extras. The adapter lives in
`galcymedia.adapters.<provider>` and imports the SDK inside its module, so
`pip install galcymedia` still installs only `websockets`, and
`import galcymedia` loads nothing third-party. The arrow goes one way,
always: the adapter knows the core, the core never knows the adapter. It is
the same separation Pipecat and LiveKit adopted in 2025 to insulate
themselves from the movement of the AI APIs, and a test watches it
(`test_importing_the_package_does_not_import_pipecat`), because the failure
is silent: one import too many breaks no test, it only makes the install
heavier until one day the SDK does not install on the customer's machine.

The maintenance boundary, worth saying out loud: we maintain the half that
touches Asterisk (the framing, the codec, the barge-in, the bridge to the
channel). The half that touches the provider's API is the provider's call.
If OpenAI renames a field, that is not a channel failure; if Asterisk
changes the channel, it is ours.

## The Pipecat adapter converts the sample rate and warns

The best case is a pipeline built at the channel's rate, and when that
happens here not a byte is touched. When they differ there are two obvious
paths: only warn (the rate is chosen by the `Dial`, and Asterisk converts in
C, where it is cheap: see "The user picks the codec"), or ask the integrator
to declare the codec in their configuration. We do a third thing: the
adapter converts in both directions and says so in the log.

Only warning does not cover this case, because the problem is one of order
and has no way out anywhere else. The `c()` of the `Dial` decides the rate,
but the channel only announces it in the `MEDIA_START`, when the pipeline
has already started. The channel's announcement admits no reply:
`chan_websocket.c:1158` sends it with `send_event`, so there is nothing to
renegotiate on Asterisk's side. And on Pipecat's side neither: its voice
engine fixes the rate at startup (`stt_service.py:359`) and has already
connected with it. The only place the adaptation fits is the serializer,
which sits in the middle. And the failure goes unnoticed, which is what
makes it expensive: an 8 kHz channel under a pipeline built at 16 kHz raises
no exception. Measured: one second of voice plays as two, that is, at half
speed and an octave down, with every meter green.

Asking the integrator to declare the codec is worse: it hands them a new way
to get wrong something Asterisk already knows. Declaring `slin44` over an
`alaw` channel delivers the voice 5.5 times slower, with their name on it.

So it converts, and it warns once per call with the exact line to write to
avoid it. The notice is a `warning` and not an `error` because it describes
no failure, only a cost: converting spends CPU on every frame and in both
directions. With the two rates equal no converter is created at all. Pinned
by `test_the_audio_is_resampled_when_the_rates_differ`,
`test_no_resampling_happens_when_the_rates_match` and
`test_the_rate_warning_is_logged_once_per_call`.

The output is converted from the rate the frame carries, not from the input
rate. The obvious thing is a single converter, built with the pipeline's
input rate. But Pipecat resamples the bot's audio to `audio_out_sample_rate`
and rebuilds the frame with that rate (`base_output.py:129`,
`fastapi.py:533`), and the pipeline's two rates need not match: a 24 kHz TTS
over a pipeline listening at 8 kHz is the usual case. Measured with the
channel in ulaw and a single input converter: one second at 24 kHz comes out
as 24,000 bytes, that is, three seconds; with input at 16 kHz and output at
8 kHz, one second comes out as half. So the output converter is picked when
encoding, from `frame.sample_rate`, independent of the input one, and it
warns once per call with its own `PipelineParams(audio_out_sample_rate=...)`
when the output rate is not the input rate. The hint in the notice names the
pipeline's rate, which is the one the channel lacks. Pinned by
`test_the_output_rate_is_the_frame_rate_not_the_input_rate` and
`test_output_at_the_channel_rate_is_not_converted_even_if_the_input_is`.

## Hanging up from a Pipecat tool: `EndAfterBotTurn`, not an `EndFrame`

The obvious way for a tool to end the call is to push an `EndFrame` right
after answering: the serializer turns it into HANGUP and the output
transport drains its audio queue first (`base_output.py:506-520`, pipecat
`072df9de`), so whatever was queued plays out. Two real calls (2026-08-22)
showed what that misses: the model answered the tool call with no text at
all, and the text came in the NEXT turn, the one the aggregator runs after
the tool result (`llm_response_universal.py:1731-1737`). That turn started
after the `EndFrame` had already passed, so its goodbye was cancelled by
the hang-up in both calls. The agent mode does not have this problem
because `finish()` waits for the bot's turn; the Pipecat side needed the
same thing.

It cannot live in the serializer: the transport writes it only audio,
messages, the interruption, the `EndFrame` and the `CancelFrame`
(`transports/websocket/fastapi.py:466-520`), never the TTS or LLM control
frames. So it is a `FrameProcessor` placed between the TTS and
`transport.output()`, armed by a `HangUpAfterTurnFrame` the tool pushes.
The marker is a `ControlFrame` (`frames.py:127`): it travels in order, and
the TTS forwards frames it does not handle through the same queue as its
audio contexts (`tts_service.py:879-888`), so it lands behind the speech of
its own turn.

The end of the turn is not "the first `TTSStoppedFrame`". Pipecat's TTS
aggregates text into sentences (`tts_service.py:310`) and a TTS over HTTP
emits one Started/Stopped pair per sentence; a two-sentence goodbye would
be cut after the first. The condition is the LLM's `LLMFullResponseEndFrame`
seen with no speech in flight (Started/Stopped pairs counted since the
marker). Pipecat's TTS holds the End and emits it after its own Stopped
(`tts_service.py:354,1629`), so with it the order downstream is always
Started, audio, Stopped, End; a TTS that does not reorder still works
because the in-flight count waits for its Stopped. And only an End that
follows a `LLMFullResponseStartFrame` seen after the marker counts: the
tool handler runs as a task (`llm_service.py:1332`) while the LLM pushes the
End of the turn that called the tool in its `finally`
(`services/openai/base_llm.py:602-604`), so that End can land behind the
marker, and taking it for the goodbye's End would hang up before the goodbye
(`test_the_end_of_the_turn_that_called_the_tool_is_ignored`). An interruption during
the goodbye hangs up at once (the transport flushed the audio anyway), and
`FINISH_MAX_WAIT_S` caps the wait when no turn comes at all: a model that
never says goodbye costs five seconds, not an open line.

One thing is Pipecat's and is felt: with a TTS over HTTP the base class
pushes the `TTSStoppedFrame` after `stop_frame_timeout_s` of idle, 3.0 s by
default (`tts_service.py:157,294`), so the hang-up lands three seconds after
the last sentence. With ElevenLabs over WebSocket the Stopped comes with
the provider's final message for the turn's context
(`services/elevenlabs/tts.py:916-920`) and there is no such wait.

## A sample is two bytes, and not every engine respects that

The adapter keeps the stray byte of an odd-length PCM block and prepends it
to the next one. It looks like paranoia and is not: engines that deliver
audio by streaming cut where the network buffer ends, not at a whole sample.

Dropping that byte, which is what the conversion does on its own, does not
lose one sample: it shifts every following sample by half a step, so each
one is assembled from halves of two neighbors. Measured on a known wave, the
signal falls from 37 dB to below zero, that is, more noise than voice. And
it raises nothing: the call simply sounds bad, which is the kind of failure
that is most expensive to diagnose.

## The dead peer is detected with the server's PING, not with audio silence

Asterisk does not warn when it dies suddenly: in 23.4.1 Asterisk's WebSocket
client sends no PING (`res_websocket_client.c` has none; the
`enable_pingpongs` option arrives with `184fafc20c`, "WebSocket
Enhancements: Proxies and Keepalives", and its first tag is `24.0.0-pre1`).
Measured in the lab, which is 23.4.1, with iptables rules dropping port 9000
traffic during a call: the `Session` and `active_calls` lived 15 min 56 s,
until TCP's retransmission timeout (`tcp_retries2=15`). A 16-minute ghost
call takes a `max_calls` slot and leaves a panel waiting.

There are two ways to detect it. The first is the server's PING
(`ping_interval_s=20`, `ping_timeout_s=20`, the `websockets` defaults).
Asterisk answers PONG to every PING it receives
(`res_http_websocket.c:689-693`), so a healthy call is not cut by the
protocol. The objection was that under load the PONG might be delayed past
20 s and a call with audio flowing be killed. Measured in the lab with the
6 cores saturated (load 6.19) and 80 calls doing echo: PONG at most 5 ms,
largest gap between audio frames 35 ms. And with our event loop blocked for
3 s (15 times the timeout) with a PING in flight, the call survives: asyncio
processes the selector events before the expired timers, so the PONG that
was already in the buffer wins. Whoever has a loop that blocks for more than
20 s has another problem before this one.

The second is inbound audio silence (N seconds without frames = dead).
Asterisk sends a frame every 20 ms while audio comes from the bridge, but
what happens with a caller on hold or with silence suppression has not been
measured, and an audio timeout would kill those live calls. It stays as the
alternative if the ping ever shows a measured false positive.

The ping was chosen. It is the protocol's mechanism for exactly this, it
does not depend on there being voice, and it is turned off with
`ping_interval_s=None`.

## `llm-function-call-started` carries `tool_call_id` and `arguments`, which the standard does not define there

In RTVI 2.1 `LLMFunctionCallStartMessageData` only carries `function_name`
(Pipecat, `processors/frameworks/rtvi/models.py:257-264`, PROTOCOL_VERSION
2.1.0); `tool_call_id` and `arguments` belong to the `in-progress` message
(`:317-319`) and `tool_call_id` is mandatory in `stopped` (`:322`). Our
`started` sends all three. They stay, on purpose: a panel paints "checking
the calendar..." with the `started` and clears it with the `stopped`, and
without the `tool_call_id` in the first it cannot pair which one to clear
when the model asks for several at once. The extras are harmless to a client
of the standard: its serializer ignores them (pydantic, `extra="ignore"` by
default). Emitting `in-progress` as well would be a third event per tool to
say the same thing.

## `TOOL_TIMEOUT_S` is 20 s, and the ElevenLabs cut is for total silence, not for the `pong`

Measured against the real service: a client that sends no message at all
receives a `ping` every 1.7 s and the close at 61.1 s with 1002 "No user
message received for 60 seconds"; with audio flowing (a Spanish sentence
every ~12 s with silence in between) and zero `pong` for 100 s, 60 pings
unanswered, the session stays open and converses normally. It matters
because while `on_function_call` runs the adapter's reader is blocked: it
processes no barge-in and no bot audio, and it does not answer ElevenLabs'
JSON `ping` (the `websockets` protocol ping answers itself), and the
suspicion was that a slow hook would cause the cut.

So the audio the session sends from its own task (`send_audio` does not go
through the reader) resets the counter, and a blocked hook does not cause
the cut: what it costs is the barge-in and the bot audio that go unprocessed
while it lasts. The cut only falls when nothing goes out, and the cap stays
at half of those 60 s as a floor for that case, which is what
`test_the_tool_timeout_stays_under_the_elevenlabs_cut` pins. The SDK
(`elevenlabs-python`, `types/ping_payload.py`) only says "sent
periodically": the number comes from measuring, not from the doc.

## Deepgram receives real-time silence when the channel goes quiet, not a `KeepAlive`

The channel writes nothing when the caller's leg delivers no frames: it
drops comfort noise (`chan_websocket.c:1214`), does not write with
direction `out` (`:1210`) and synthesizes no silence. Measured in the lab
with a probe counting frames per second: 50/s during a `Playback()` and 0/s
during 14 s of `Wait()`. A phone with silence suppression, a hold or any
dialplan application on the caller's side look the same from the WebSocket:
voice, and then nothing.

The SDK's `KeepAlive` is the answer Deepgram offers for that gap. It does
not work here, and the reason is what Deepgram does with that "nothing",
measured against the real service (`nova-3`, `gpt-4o-mini`, `aura-2`) with a
3 s sentence recorded in ulaw and sent at real-time pace, one 160 B frame
every 20 ms. Times from the end of the sentence:

| After the sentence | User `ConversationText` | Bot reply | `AgentAudioDone` |
|---|---|---|---|
| (g) continuous silence, like a phone without VAD | +0.5 s | +2.1 to +3.2 s | +2.9 to +4.0 s |
| (d) nothing | **never** (only `UserStartedSpeaking`); `Error` `CLIENT_MESSAGE_TIMEOUT` at +12.3 s, close 1005 | never | never |
| (e) `{"type": "KeepAlive"}` every 5 s | **never** in 40 s, with the socket open | never | never |
| (f) silence tail of 500, 1000, 1500, 1750 or 1900 ms, then nothing | +0.4 s | **never** | never |
| (f) tail of 2000 ms | +0.4 s | +2.07 s, the instant the tail ends | +3.1 s |
| (f) tail of 2500 ms | +0.4 s | +2.14 s | +2.95 s |
| (h) 5 s of nothing, then continuous silence | +5.43 s (0.43 s after resuming) | +7.1 s | +8.1 s |
| (h) 9 s of nothing, then continuous silence | +9.46 s | +11.1 s | +12.1 s |

Three things come out of the table. A `KeepAlive` keeps the socket and
leaves the bot mute: (e) is the caller with silence suppression, who talks,
goes quiet, and the turn never closes. The message serves what its SDK says,
"idle periods" (`agent/v1/requests/agent_v1keep_alive.py`; its guide: "emit a
`KeepAlive` every ~5 seconds. Without it, the server closes the socket at
~10 seconds of idle"), not a half-finished turn. There is no such thing as a
"short tail" of silence: the edge is at 2.0 s and coincides with the LLM's
latency (2.07 s against 2.1 s in the baseline), so it is not a threshold of
the end-of-turn engine. (h) confirms it: the whole pipeline (transcription,
reply, voice) advances with the clock of the incoming audio, and a gap of N
seconds shifts everything by N seconds. A fixed tail depends on how long the
model or a tool takes that day. And the `LatencyReport`s that come out with
the silence are not a defect: they show up just the same in (g), they are
the STT's normal telemetry.

So the adapter does what a phone without VAD does: from `GAP_BEFORE_FILL_S`
(300 ms) without a caller frame it sends one codec-silence frame per
`ptime`, and stops at the first real frame (checked before each fill frame:
the maximum overlap is one frame). The 300 ms come from measuring: under
load the largest gap between real frames was 21 ms, and being wrong is
cheap, because a burst of SIP loss past 300 ms only produces a few extra
silence frames followed by the voice. Waiting longer costs reply latency,
second for second, per (h). With audio flowing the `KeepAlive` is redundant:
the audio is the message. Pinned by
`test_the_filler_is_silence_paced_at_ptime`,
`test_the_filler_stops_on_the_first_real_frame` and
`test_no_filler_while_the_channel_delivers`.

Before writing it, the SDK (`deepgram-python-sdk` v7.7.0, commit `921983a`,
paths under `src/deepgram/`) was searched for another way out the provider
might offer, and there is none. An end-of-turn knob that closes without tail
audio, in `listen` v1, does not exist: `DeepgramListenProviderV1` only has
`type`, `version`, `model`, `language`, `keyterms` and `smart_format`
(`types/deepgram_listen_provider_v1.py:7-27`). `eot_threshold` and
`eager_eot_threshold` exist only in v2, the `flux` family
(`types/deepgram_listen_provider_v2.py:38,43`), and they do not change that
the pipeline advances with the audio. Its guide says nothing about silence
suppression or about sending silence; the only thing on pauses: "Resume
after pause: just call `send_media` again. No control message is required
[...] the agent picks up VAD on the next chunk"
(`.agents/skills/deepgram-python-voice-agent/SKILL.md:225`). And
`socket_client.py` does not handle periods without audio: it only exposes
`send_keep_alive` (`agent/v1/socket_client.py:185`, `:332`), with no timers
and no pacing of its own.

And the same in ElevenLabs, measured the same way (3 s sentence at real-time
pace, then nothing, with the `pong`s answered): no `user_transcript` in
40 s. With `{"type": "user_activity"}` every 5 s, which its SDK describes as
"ping to prevent timeout" (`conversation.py:59`, `register_user_activity`
`:849`), neither. ElevenLabs' pipeline advances only with incoming audio, so
the quiet-channel fill is needed there too.

What the fill does not know about is Deepgram: it is the channel's, which
goes quiet when the caller goes quiet. That is why the fill lives in
`_shared.SilenceFiller` and both adapters use it. Each one hands it a `send`
that puts one frame on the wire through the same path as the real audio (in
ElevenLabs: transcoded and in base64) and that does not report the frame
back: a fill that reset the gap clock would switch off and on every 300 ms.
The byte is the channel's silence; in ElevenLabs it goes through the same
table as the phone's silence (alaw `0xD5` lands in ulaw `0xFE`, amplitude 8
over 32,767), which is exactly what the provider hears when the caller goes
quiet without VAD. OpenAI, measured the same way (`gpt-realtime`,
`server_vad`): with continuous silence, `speech_stopped` at +0.24 s,
transcript at +0.81 s, audio at +1.14 s; after the sentence and nothing, no
`speech_stopped` and no reply in 40 s (the socket is not cut, but the
server's VAD needs input silence to close the turn). All three adapters use
the same `SilenceFiller`.

A consequence the integrator has to know: with the fill, a long hold looks
to the agent like a quiet person, and the ElevenLabs agent hangs up on its
own at ~39 s of continuous silence by its dashboard's policy (measured:
"Parece que no hay nadie. Que tengas un buen día", that is, "Looks like
nobody is there. Have a good day", close 1000). That is a
decision of the provider's dashboard, not of the library.

## Pipecat closes the turn on its own when the channel goes quiet: `audio_idle_timeout`, not a fill

The channel writes nothing while the caller is quiet (see "Deepgram
receives real-time silence"). A fill like the other two adapters' is
redundant here, because Pipecat has what the providers do not. Its VAD runs
inside the pipeline and, like the providers', only changes state with audio:
`analyze_audio` goes to `QUIET` after `stop_secs` of silent frames
(`vad_analyzer.py:242-246`), and without frames there is no transition. But
it also has a watchdog: `VADController._audio_idle_handler`
(`vad_controller.py:194-215`) forces the end of speech when no audio arrives
for `audio_idle_timeout` (1.0 s by default, `:75`), with a
`WARNING "no audio received while speaking, forcing speech stop"` each time.

Measured on 2026-08-21 with Pipecat v1.7.0-222's `SileroVADAnalyzer` +
`VADController` (3.7 s sentence with no silence tail, at real-time pace,
20 ms per frame), from the last voiced frame:

| After the sentence | `on_speech_stopped` |
|---|---|
| (g) continuous silence | +0.20 s (`VAD_STOP_SECS`) |
| (d) nothing, default `audio_idle_timeout` | +1.01 s, with the warning |
| (d) nothing, `audio_idle_timeout=0.3` | +0.31 s (two runs) |

That measurement drove `VADController` alone, so it times the VAD's stop, not
the turn's end: in the pipeline the `VADUserStoppedSpeakingFrame` (at
`stop_secs`, or forced by the watchdog) makes the default strategy, Smart
Turn (`user_turn_strategies.py:43-51`), run `analyze_end_of_turn`
(`turn_analyzer_user_turn_stop_strategy.py:238`), and the turn closes there
only on `COMPLETE`; on `INCOMPLETE` the strategy returns without closing
(`:339-340`) and what remains is Smart Turn's own 3 s of silent frames
(`base_smart_turn.py:27,128-136`), which a mute channel never delivers, or the
aggregator's `user_turn_stop_timeout` of 5.0 s (`llm_response_universal.py:172`,
`user_turn_controller.py:378-391`). So `audio_idle_timeout=0.3` shortens the
`COMPLETE` path only; an `INCOMPLETE` on a mute channel waits about 5 s.

That is why the Pipecat adapter does not fill: a serializer has no clock
(the transport calls it per message) and none is needed. What is needed is
the kwarg, which exists since Pipecat 1.0.0 and is a parameter of
`LLMUserAggregatorParams` (`llm_response_universal.py:169`):
`audio_idle_timeout=0.3`, the same criterion as the other adapters'
`GAP_BEFORE_FILL_S`. The transcript is not left half done: Pipecat's
Deepgram STT sends `Finalize` when speech closes
(`services/deepgram/stt.py:785-789`). The `04-pipecat` example carries the
kwarg.

Mind the test sentence: a WAV that ends in silence closes the turn before
its last frame and the scenarios stop being distinguishable. That is why the
measurement trims the tail.

## OpenAI's truncate only goes out while the bot is playing

The obvious thing is to send `conversation.item.truncate` on every barge-in,
with the `item_id` of the last audio and how much had played; or to clear
that `item_id` on `response.output_audio.done` so a later barge-in has
nothing to truncate. We do something else: the truncate goes behind
`speech.bot_audio_playing`, the only signal that knows whether the bot is
still playing (`_speaking` or `_audio_pending`, and the latter goes off only
with the channel's `QUEUE_DRAINED`). And the item is not cleared on
`response.output_audio.done`, because there the aligner and Asterisk's queue
are still playing and an interruption in that stretch is a real barge-in.

Measured against the real service, truncating on every barge-in with the
reply played whole and 3 s of silence before the next sentence: 2 normal
turns, 2 truncates sent (15210 of 15250 ms, 13510 of 13550), 2
`conversation.item.truncated` back, 0 errors. The server accepts it, because
the value does not exceed the real one, and deletes the server-side
transcript of a sentence the person heard in full (its SDK: "Truncating
audio will delete the server-side text transcript"). With a partial truncate
(7287 of 11750 ms) the bot, asked to repeat, omitted exactly what came after
the cut: the context is lost. Pinned by
`test_openai_does_not_truncate_a_reply_heard_whole`, with its positive
control.
