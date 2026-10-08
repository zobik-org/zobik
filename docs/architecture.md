# Architecture document: Zobik - decentralized ecosystem of AI agents

---

## Editing guidelines for this document

> **Note for whoever edits, not for whoever reads the architecture.** This section is not part of the specification: it fixes how the rest of the document must be written. It is not numbered—so as not to interfere with the section numbering the document uses as its cross-reference system—and no other section may cite it.

**The general documentation guidelines apply**, which live in [writing-guidelines.md](writing-guidelines.md) and are not repeated here: a document describes a state, not a journey; everything is said once; items are not counted; things are defined by what they are; between two sentences that say the same thing, the simpler one wins; and the conventions on identifiers and terminology. What follows applies only to this document.

**The conclusion is stated, not the path that led to it.** The document says what the architecture does and how; it does not reconstruct the reasoning behind that shape or walk through the options left out. No *"if X were done, Y would happen, so Z is done"*, and no comparisons with approaches the architecture does not implement. When a decision does not stand on its own, a brief justification is enough—one sentence naming the current trade-off—and it ends there: the reader does not need to follow the deliberation to apply the rule. Nor does the document over-explain what the statement already says; a precise definition is not illustrated three times or paraphrased in the next paragraph.

**The enforceable property is fixed, not the mechanism that provides it.** The document states what must hold and who owes it; it does not name the concrete piece that achieves it—that belongs to [implementation.md](implementation.md). The exception is narrow and has a test: the document fixes the mechanism only when two sides that are versioned and deployed separately have to agree without being able to negotiate with each other—there the choice stops being free and becomes a contract, as in the Node Runtime Interface (§3.6.1) or in the envelope and the event lifecycle (§3.8). Outside that case, naming a technology adds no precision: it adds a claim about the world that the document does not verify and that ages on its own. The test to tell them apart is the same as in the other guidelines: if deleting the name loses a verifiable constraint, it was not a product but a contract; if it only loses an illustration, it goes.

**Examples and concrete role names are confined to §5**; the body talks about nodes, requesters and workers (§2.1).

---

## 1. Executive summary
This document defines the technical architecture of **Zobik**, formalizing the move from a centralized orchestration model (rigid graphs) to a dynamic **Compound AI System** built on an **Event-Driven Architecture**. The goal is to create an autonomous, highly scalable AI Swarm, with dynamic cost optimization and resilience to systemic or LLM failures.
