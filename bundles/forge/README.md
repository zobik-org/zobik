# bundles/forge

The Forge starter bundle: common source and one adapter per language model provider.

* `openai/` is the OpenAI variant's model slot (implementation §7.2): the interface of the Responses API (`responses.interface.json`), the rate table it brings (`rates.json`) and the Go package that embeds them with the proposed entry; and the model adapter, `zobik_openai.py`, a LangChain chat model over `call`, with its dependencies pinned in `requirements.txt` from `requirements.in`. Its tests run against a fake task: `PYTHONPATH=../../../sdk/python/src:. python -m unittest discover -s tests`.
