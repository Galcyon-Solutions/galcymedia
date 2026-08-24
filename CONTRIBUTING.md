# Contributing

Five rules hold this project together, and they are the ones every line here
went through. The rest is ordinary.

## The five rules

**1. Every claim carries its source.** A claim about the channel cites the
line of `chan_websocket.c` at the Asterisk tag the README names (today
23.4.1): `chan_websocket.c:811-813`, not "Asterisk flushes everything". A
claim about a provider cites the SDK file with its version
(`elevenlabs-python` v2.64.0, `conversation.py:621`). A claim about what
happens on a call cites the measurement: date, conditions, numbers. Without
one of the three the PR comes back with a question, because the docs of this
channel are young and several things they say turned out to differ from the
source.

Line numbers are for OTHER people's code, pinned by tag or version. A
citation to OUR code goes by NAME: the function, the method, the test, the
constant. Our line numbers move on the next edit and the prose keeps citing
where the code used to be, which is worse than no citation, because it reads
as evidence. Both styles were in this repository and the count settled it:
of the internal citations by line, most had already rotted; of the several
hundred by name, none.

**2. Every fix carries the test that proves it.** The test fails without the
change and passes with it, and you checked both. A test that passes either
way protects nothing; it happened here, and those tests had to be rewritten.
Say in the PR which line you disabled to see it go red.

**3. Anything that touches audio or turn-taking gets a real call.** Paste the
log with its `[user]` and `[assistant]` lines, the Asterisk version and the
`Dial` string. A green suite checks what someone thought to check; the real
call shows what nobody did.

**4. Code, logs and tests in English.** Identifiers, log messages, exception
messages and test names. A comment exists to warn about a trap, with the
citation or the test that pins it; it does not narrate what the code does.
No em dashes or en dashes anywhere: comma, period, colon, or a plain hyphen
with spaces.

**5. Use whatever tools you want, AI included, and sign what you send.** You
must be able to explain every line and every citation in review. "The tool
wrote it" is not an answer.

## Who reviews

The maintainer is a VoIP engineer. Nothing merges without a justification,
nothing merges at the cost of voice quality, and style changes without a
measured reason are not accepted.

## The practical part

```bash
pip install -e ".[dev]"
python -m pytest -q
```

Extras: `pipecat` installs `pipecat-ai`; `deepgram`, `openai` and
`elevenlabs` install nothing, because those adapters speak their WebSocket
through `websockets` alone. Without `[pipecat]` the suite skips
`tests/test_adapter_pipecat.py` whole and two tests in `test_events.py`
(`test_all_the_types_exist_in_the_specification`,
`test_the_constants_match_the_specification`), all of them comparisons
against the installed SDK. On Python 3.13+ without `[dev]`, two tests in
`test_pcm.py` skip for lack of `audioop`; the `dev` extra brings
`audioop-lts` and they run.

One rule holds the architecture: `src/galcymedia/` imports nothing but
`websockets`. Each adapter imports its own SDK inside its module, so
`import galcymedia` loads no third-party code; a test pins that.

An adapter for another provider is welcome here, in `adapters/`: its SDK
imported inside the module, its extra in `pyproject.toml`, and the real call
of rule 3. What you send is contributed under Apache-2.0, with no CLA.

## A useful bug report

The Asterisk version, the `Dial` string (codec included), the provider and
adapter, and the log of the call with its `[user]` and `[assistant]` lines.
With those four the bug is usually reproducible in one try; without them it
is a guess.
