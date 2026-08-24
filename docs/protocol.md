# The `chan_websocket` protocol

Technical reference for the Asterisk 23 channel the library talks to. It is
what you consult once you have decided to use galcymedia and need the exact
fact: which command does what, which event arrives when, the limits, and the
channel gotchas you have to know in order not to break the audio.

Citations to `chan_websocket.c` are from **Asterisk 23.4.1**. For the *why*
of our design decisions, see [decisions.md](decisions.md).

## How audio and control travel

The channel opens a WebSocket. **Audio** goes in **binary** frames, raw
samples: no RTP, no headers, no timestamps. **Control** goes in **text**
frames (JSON). The two never mix. The application sends commands; Asterisk
sends events.

There are two control formats, a single-line one and JSON. The library
speaks **JSON only**, and not by preference:

- The source calls the old format "the legacy single-line message format"
  (`chan_websocket.conf.sample:6`). It is not deprecated, but it is the
  legacy one.
- Only JSON carries `correlation_id`, without which a `MARK_MEDIA` cannot be
  paired with its `MEDIA_MARK_PROCESSED` (`chan_websocket.c:275,305`).
- Only JSON carries `channel_variables` in `MEDIA_START`
  (`chan_websocket.c:239`), which is how the dialplan says which provider
  handles the call.

It is enabled with `control_message_format = json` in `chan_websocket.conf`
(`chan_websocket.c:2007`), or with `f(json)` in the dial string
(`chan_websocket.c:1537`).

## Limits

| Limit | Whose | Value | What happens past it |
|---|---|---|---|
| Control message (text) | the channel's (`chan_websocket.c:144`) | **128 bytes** | Asterisk drops it **without a word on the socket**: only a WARNING in the Asterisk log (`chan_websocket.c:922-924`). From the application the symptom is a command that "does nothing". |
| WebSocket message (any) | the channel's (`http_websocket.h:105`) | **65535 bytes** | `res_http_websocket` closes with 1009 (`res_http_websocket.c:672,693,721`) and the channel hangs the call up on a read error (`chan_websocket.c:1089`). |
| The library's margin | galcymedia's (`protocol.MAX_WEBSOCKET_MESSAGE_BYTES`) | **65500 bytes** | The ceiling the library uses so it never reaches the channel's limit: passthrough audio is split at this edge. |
| Asterisk's audio queue | the channel's (`chan_websocket.c:141`) | **1000 frames** (~20 s) | This is the real-time budget. It is why a garbage-collected language is fit for the job. |

## Flow control: XOFF / XON

Asterisk protects its outbound queue with two events:

- **`MEDIA_XOFF`** when the queue passes **900** queued frames: stop sending
  audio, or the channel drops it and it sounds choppy.
- **`MEDIA_XON`** when it falls below **800**: sending can resume.

The thresholds are compiled into the channel, not configurable
(`chan_websocket.c:141-143`).

**Gotcha: the XON is only emitted from the channel's timer**: the check lives
in `dequeue_frame` (`chan_websocket.c:486-490`), and that function is only
called by the timer read (`chan_websocket.c:594-602`). There are two ways to
stop that clock, and with either one an active XOFF **is never lifted**:

- `PAUSE_MEDIA` returns before the check (`chan_websocket.c:477-479`).
- `SET_MEDIA_DIRECTION=in` closes the timer altogether
  (`chan_websocket.c:888-893`).

So whoever waits for the XON has to bound the wait: if it does not arrive
within a few seconds, it is not going to.

**Version note: the phantom XOFF after a flush is gone.** In Asterisk 22.0
to 22.5 (and 20/21 before the fix), `FLUSH_MEDIA` emptied the queue but did
not reset the internal `frame_queue_length` counter (issue #1304): after a
barge-in the channel believed the queue full, rejected the new audio and the
`MEDIA_XON` never came. The fix (PR #1303) resets the counter,
`bulk_media_in_progress` and `leftover_len` under the queue lock
(`chan_websocket.c:811-816`); it is in the whole 23.x series and in 22.6.0+.
Verified live against 23.4.1 (3 fill + flush cycles): `queue_length` goes
back to 0 and the XON arrives in 2-20 ms. It is one more reason for the
Asterisk >= 23.4.0 requirement: against a 22.0-22.5 this bug cannot be fixed
from the client, because the rejection happens on Asterisk's side.

## Commands (application → Asterisk)

There are 11 (`chan_websocket.c:129-139`), case-sensitive. **In passthrough
mode the channel rejects 8 of the 11** with an `ERROR` event (macro
`ERROR_ON_PASSTHROUGH_MODE_RTN`, `chan_websocket.c:678-687`, used in
`:740-846`): only `ANSWER`, `HANGUP` and `GET_STATUS` survive.

| Command | Passthrough | What for |
|---|---|---|
| `ANSWER` | ✅ | Answers the call. |
| `HANGUP` | ✅ | Hangs up the bot's leg. |
| `GET_STATUS` | ✅ | Asks for the real queue depth. Arrives as a `STATUS` event. |
| `REPORT_QUEUE_DRAINED` | ❌ | Asks to be told when the queue runs empty. |
| `SET_MEDIA_DIRECTION` | ❌ | Changes which half of the audio stays alive (`both`/`in`/`out`). |
| `START_MEDIA_BUFFERING` | ❌ | Lets Asterisk assemble the frames instead of you. |
| `STOP_MEDIA_BUFFERING` | ❌ | Closes buffering and sends the pending remainder. |
| `MARK_MEDIA` | ❌ | Puts a marker in the audio queue. |
| `FLUSH_MEDIA` | ❌ | Discards everything queued (the barge-in piece). |
| `PAUSE_MEDIA` | ❌ | Stops playback without discarding what is queued. |
| `CONTINUE_MEDIA` | ❌ | Resumes after `PAUSE_MEDIA`. |

## Events (Asterisk → application)

There are 9: the channel's `_create_event_*` (`chan_websocket.c:198-430`),
not one more.

| Event | Carries | When |
|---|---|---|
| `MEDIA_START` | `format`, `optimal_frame_size`, `ptime`, `channel_variables` (`chan_websocket.c:231-239`) | Once per call, normally before the audio (see the gotcha below). |
| `DTMF_END` | `digit` | A keypad digit, out of band (not as a tone in the audio). |
| `MEDIA_XOFF` | - | The queue passed 900 frames: stop sending. |
| `MEDIA_XON` | - | The queue fell below 800: resume. |
| `STATUS` | queue depth | Reply to `GET_STATUS` (`chan_websocket.c:360-369`). |
| `MEDIA_BUFFERING_COMPLETED` | `correlation_id` | Reply to `STOP_MEDIA_BUFFERING`. |
| `MEDIA_MARK_PROCESSED` | `correlation_id` | Everything queued before the mark has played. |
| `QUEUE_DRAINED` | - | The queue ran empty (reply to `REPORT_QUEUE_DRAINED`). |
| `ERROR` | detail | The channel rejected a command. |

**There is no hangup event.** The channel emits these 9 and none of them
announces the end of the call (verified in the `_create_event_*` list). The
only thing that arrives is the WebSocket close code, which Asterisk picks on
purpose:

| Code | Meaning | Where |
|---|---|---|
| 1000 | Orderly end: the application asked for `HANGUP`, or the channel hung up and closed with `hangupcause ?: 1000`. The real calls of 2026-08-22 all closed with 1000, including one where the provider hung up on its own. | `chan_websocket.c:737`, `:1756` |
| 1001 | The network went away, or the other side sent its own close frame. | `chan_websocket.c:1089`, `:1113` |
| 1003 | A frame of a type the channel does not accept arrived. | `chan_websocket.c:1127` |
| 1005 | No close frame: what the WebSocket client reports when the connection goes away without one. It does not come from the channel's source; observed. | - |

Every event carries `channel_id` (`chan_websocket.c:204` in the catch-all
`_create_event_nodata`, and `:235,:252,:274,:304,:334,:362,:413` in the ones
that carry data), which lets a shared hook tell concurrent calls apart.

## Marks vs the queue-drained notice

Both serve for "tell me when this audio has played", but they are destroyed
differently, and that difference decides which one to use:

- **`MARK_MEDIA`** queues a control frame next to the audio
  (`chan_websocket.c:798-802`). **`FLUSH_MEDIA`** frees EVERY frame in the
  queue without looking at its type (`chan_websocket.c:811-813`): a mark in
  flight is destroyed and its ack never arrives.
- **`REPORT_QUEUE_DRAINED`** raises a **channel flag**
  (`chan_websocket.c:823`), not a frame in the queue. A flush does not touch
  it: on the contrary, emptying the queue brings the notice forward.

Practical consequence: to wait for the end of the audio **when a flush may
happen in between** (the barge-in followed by hangup case), use the
queue-drained notice, not a mark.

## Buffering: letting Asterisk assemble the frames

`START_MEDIA_BUFFERING` (`chan_websocket.c:740-743`) makes the channel keep
the remainder that does not complete a frame and stitch it to the next
message (`chan_websocket.c:970-1001`). It is frame alignment, on Asterisk's
side. It has to be closed with `STOP_MEDIA_BUFFERING`, which queues the last
remainder (`chan_websocket.c:765-772`), or that remainder never plays.

**Outside buffering the channel does NOT pad or keep: it drops.** From each
binary message it cuts whole frames of `optimal_frame_size`
(`chan_websocket.c:1036-1044`) and the tail that does not complete one is
thrown away (`chan_websocket.c:1031-1033`). That is why the library aligns
the frames before sending them (`framing.py`).

The closing notice (`MEDIA_BUFFERING_COMPLETED`) travels as a frame in the
queue (`chan_websocket.c:773-777`), so a `FLUSH_MEDIA` destroys it, same as
the marks.

## The codecs and passthrough mode

The channel is codec-agnostic: the one you pick in the `Dial` with
`c(<codec>)` is the one that travels over the WebSocket. The name that
arrives in `format` is the one `ast_format_get_name` writes
(`chan_websocket.c:236`), that is, the `.name` in `codec_builtin.c`: `ulaw`
(`:168`), `alaw` (`:183`), `slin` (`:288`) and `slin12`..`slin192` from
`format_cache.c:397-407`. **`pcma`, `pcmu` and `mulaw` never arrive**: they
are names from other worlds (SDP, providers), and a table that accepts them
has dead entries.

One line splits the codecs into two worlds, and it is NOT by name: it is by
minimum frame size. In the source (`chan_websocket.c:1406-1408`):
`if (native_codec->minimum_bytes <= 10) { passthrough = 1;
optimal_frame_size = 0; }`. For the rest, `optimal_frame_size = default_ms *
minimum_bytes / minimum_ms` (`chan_websocket.c:1410-1412`), with the three
values from `codec_builtin.c`.

| Codec in the `Dial` | `minimum_bytes` (`codec_builtin.c`) | `optimal_frame_size` (20 ms) | Passthrough | What the WebSocket receives |
|---|---:|---:|:---:|---|
| `alaw` / `ulaw` | 80 (`:190`, `:175`) | 160 B | no | opaque G.711, 8 kHz |
| `slin` | 160 (`:295`) | 320 B | no | linear PCM 8 kHz |
| `slin16` | 320 (`:327`) | 640 B | no | linear PCM 16 kHz |
| `slin24` | 480 (`:343`) | 960 B | no | linear PCM 24 kHz |
| `g722` | 80 (`:676`) | 160 B | no | **compressed** G.722 (has to be decoded) |
| `codec2` | 6 (`:129`) | 0 | **yes** | compressed audio |
| `lpc10` | 7 (`:449`) | 0 | **yes** | compressed audio |
| `g729` | 10 (`:473`) | 0 | **yes** | compressed audio |
| `speex` / `speex16` / `speex32` | 10 (`:603`, `:621`, `:639`) | 0 | **yes** | compressed audio |
| `opus` | 10 (`:779`) | 0 | **yes** | compressed audio |
| `alaw` / `ulaw` with `p()` in the `Dial` | 80 | **160 B** | **yes** (`:1536`, `:1632-1634`) | opaque G.711, and the channel rejects the commands all the same |

Seven codecs fall into passthrough on their own: the ones with
`minimum_bytes <= 10` in `codec_builtin.c` 23.4.1. The number decides, not
the name: that is why `slin16` (large frames) keeps the full API just like
`alaw`, while `opus` (tiny frames) does not.

**The runtime signal depends on how you got in.** With a small codec,
`optimal_frame_size` arrives as **zero** in the `MEDIA_START`. With the
`p()` option of the `Dial` (`chan_websocket.c:1536`), the channel is in
passthrough (`:1632-1634`) but `optimal_frame_size` is still the codec's:
the `MEDIA_START` does not give it away. The only thing that does is the
first rejection, an `ERROR` with the text
`not supported in passthrough mode` (`chan_websocket.c:681`), and that is
what the library uses to find out (`session.py`,
`test_a_p_option_dial_is_detected_from_the_channel_error`).

**In passthrough the channel rejects 8 of the 11 commands**: buffering,
marks, flush, pause, direction and the queue-drained notice. Barge-in and
clean hangup are lost, and with a small codec the audio is compressed on top.
That is why `opus`/`g729`/`speex` and company are not an option with this
channel: transcode them in Asterisk to `slin16` or `ulaw`. And `p()` with
G.711 adds nothing: the audio is already opaque without it, and only the
commands are lost.

A note on `g722`: it keeps the full API (it does not fall into passthrough),
but the channel writes the bytes in the native codec without transcoding, so
the WebSocket receives **compressed** G.722, which the client would have to
decode. For 16 kHz audio, `slin16` (raw PCM) is the better choice over
`g722`.

Which codec to pick per provider and trunk, with the evidence for each row:
the README table, section "Which codec to put on the WebSocket".

## MEDIA_START may arrive after the audio

In a small fraction of calls the binary audio arrives **before**
`MEDIA_START` (Asterisk issue #1712), and it gets worse above 60% CPU. Any
consumer of the channel has to tolerate the audio showing up first instead
of taking for granted that `MEDIA_START` already happened. The library drops
those first frames instead of blowing up against a provider that does not
exist yet.

## The "dangerous" dialplan functions do not work after the `Dial`

The fact, in the words of Asterisk issue 2038 (2026-07-23, closed): "Once a
Dial to a chan_websocket destination has been performed, Asterisk will
refuse to execute any "dangerous" functions from the dialplan". The log says
`ast_func_read: Dangerous function STAT read blocked` (`pbx_functions.c`).
The issue speaks of dangerous functions in general, with `STAT` as the
example; it does not mention `func_curl`, AMI or ARI. `CURL` is one of those
functions (its own doc points at `live_dangerously`, `func_curl.c:90`), so
it falls under the same block.

What the source shows, without attributing the marking to `chan_websocket`:

- The block is applied by `ast_thread_inhibit_escalations()`
  (`pbx_functions.c:494`), a per-thread flag.
- The one that raises it is `handle_tcptls_connection()` (`tcptls.c:152`),
  for EVERY TCP/TLS connection, because "TCP/TLS connections are associated
  with external protocols, and should not be allowed to execute 'dangerous'
  functions" (`tcptls.c:147-150`).
- PR 2039, which closes the issue, explains the path: an OUTBOUND connection
  runs that function on the thread of whoever asks for it, and after an
  outbound `Dial` to `WebSocket/` that thread is the PBX's, which stays
  marked for the rest of its life. The fix limits it to inbound connections
  (`tcptls.c:264-270` in `23.5.0-rc1`). In **23.4.1 the bug is still there**:
  no `git tag --contains` of the fix below `23.5.0-rc1`.

That path (outbound Dial, PBX thread marked) is the one the PR describes; it
has not been verified here with a trace of our own. What is verified is the
effect: the function fails after the `Dial`. That is why the way back to the
dialplan is **FastAGI**, which does not go through that list. The full *why*
of that choice, against AMI and ARI, is in [decisions.md](decisions.md).
