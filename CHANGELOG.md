# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and the versioning is [semantic](https://semver.org/).

## 0.1.0 - 2026-08-24

First release. Asterisk 23.4.0 or later with `chan_websocket`; Python 3.10
or later; `websockets` as the core's only dependency.

### Added

- The server (`serve()`, `connect()`) and the session over `chan_websocket`:
  the channel's 11 commands and 9 events in JSON (`f(json)`), XOFF/XON flow
  control with a bounded wait (`xoff_max_wait_s`), frame alignment to the
  `optimal_frame_size` the channel announces, and the channel's limits
  honored: 128 bytes per control message, 65535 per WebSocket message with
  a margin at 65500, a 1000-frame queue
  (`test_the_protocol_tables_list_every_command_and_event`, docs/protocol.md).
- Turn-taking (`session.speech`): the barge-in flushes Asterisk's queue and
  discards the audio of the cancelled reply, the ceiling of what was heard
  feeds OpenAI's truncate, the goodbye is not cut because `finish()` waits
  for the bot's turn and the queue-drained notice, and passthrough mode is
  detected by small-frame codec and by `p()` in the `Dial`
  (`test_bot_audio_playing_stays_after_end_turn_until_drained`,
  `test_a_p_option_dial_is_detected_from_the_channel_error`,
  docs/architecture.md, "Turn-taking").
- Four adapters, installable through extras: Deepgram Voice Agent, OpenAI
  Realtime, ElevenLabs Agents (native ulaw, alaw transcoded by the adapter)
  and a serializer for Pipecat, which converts the sample rate when the
  pipeline was not built at the channel's and warns once per call
  (`test_the_audio_is_resampled_when_the_rates_differ`). The three voice
  providers are fed real-time silence when the channel goes quiet
  (`test_the_filler_is_silence_paced_at_ptime`).
- `EndAfterBotTurn` and `HangUpAfterTurnFrame` for Pipecat: a tool that
  ends the call pushes the marker instead of an `EndFrame`, and the
  processor hangs up once the bot's next turn has played, an interruption
  cut it, or `FINISH_MAX_WAIT_S` passed, the `finish()` semantics of the
  agent mode. An `EndFrame` pushed from the tool hung up before the model's
  answer to the tool result, which is the turn where it says goodbye (two
  real calls, 2026-08-22;
  `test_it_hangs_up_after_the_whole_response_not_at_the_first_stopped`).
- RTVI 2.1 events, fourteen, listed by `describe_events()`
  (`test_the_published_event_count_matches_the_catalog`), with `on_event`
  and `on_provider_event` as raw peepholes into the channel and the provider.
- FastAGI (`AgiServer`, `AgiRouter`) for the way back to the dialplan, and
  `Transfers` with `CallDecisions` so the bot's decision crosses into
  `extensions.conf` through `SET VARIABLE`
  (`test_the_closing_handler_keeps_what_the_dial_agi_wrote`). A decision
  `handler` applied on a call is never overwritten by a later AGI of the
  same call, whichever path it comes through: the default is written on the
  first ask only (`test_each_decision_is_delivered_once`, seen in a real
  call on 2026-08-22 where `CDR(userfield)` read `hangup` for a transferred
  call).
- `ChannelObserver`, the per-call channel trace, capped, evicting the least
  active call (`test_eviction_drops_the_least_recently_active_call`).
- `pcm`, the G.711 to linear PCM bridge without `audioop`, with tables
  verified against the reference on all 65,536 values
  (`test_matches_the_reference_exactly`).
- Server-side PING/PONG keepalive toward Asterisk, because in 23.4.1
  Asterisk's WebSocket client sends no ping (docs/decisions.md, "The dead
  peer is detected with the server's PING").
