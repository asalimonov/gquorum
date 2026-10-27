# gquorum project info

## Documentation rules

- Documentation is markdown files WITHOUT line lenght limit, we are not in ancient period with 15" CRT screens.
- Architectural and high-level information is in `./docs/architecture`, namely:
  - design principles and constraints
  - projejct structure overview
  - high-level and/or user visible APIs and methods overview (not their documentation)
  - don't describe complex relations in text, use mermaid or ascii diagrams
  - simple and short are better walls of text
  - each section or a paragraph shouldn't be more 2500 symbols
- Package specific information
  - complex internals, algorithms, descriptions of side-effeects of using functions should be in README.md in that package
  - don't descripe what is obvious from the code, its wasting of time and resources
  - comments of functions can contain up to 4 lines of comments to describe some complexities or side-effects of a function
  - README.md shouldn't contain more than 10.000 words. Compact texts or propose package splitting on child packages.

## Code review

Read `./.revmux/profile.md` and use rules from there.
