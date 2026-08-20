# Third-party notices

Zeron bundles the following syntax-highlighting components. Their parsers and
queries are consumed from the pinned Rust crates listed in `Cargo.lock`.

| Component | Version | License | Source |
| --- | --- | --- | --- |
| Tree-sitter | 0.26.11 | MIT | https://github.com/tree-sitter/tree-sitter |
| Tree-sitter highlight | 0.26.11 | MIT | https://github.com/tree-sitter/tree-sitter |
| Tree-sitter Rust grammar and queries | 0.24.2 | MIT | https://github.com/tree-sitter/tree-sitter-rust |
| Tree-sitter JavaScript grammar and queries | 0.25.0 | MIT | https://github.com/tree-sitter/tree-sitter-javascript |
| Tree-sitter TypeScript grammar and queries | 0.23.2 | MIT | https://github.com/tree-sitter/tree-sitter-typescript |
| Tree-sitter Python, Go, JSON, Bash, HTML, CSS, C, C++, C#, Java, Ruby and PHP grammars and queries | pinned in `Cargo.lock` | MIT | https://github.com/tree-sitter |
| Tree-sitter TOML, Markdown, YAML, Kotlin, Swift, SQL, Lua, Nix, Make and Containerfile grammars and queries | pinned in `Cargo.lock` | MIT-compatible; see each crate | Crate repositories recorded in `Cargo.lock` |

## Embedded fonts

Zeron embeds the following fonts in the desktop binary:

| Component | Version | License | Source |
| --- | --- | --- | --- |
| Geist and Geist Mono | bundled release | SIL OFL 1.1 | https://github.com/vercel/geist-font |
| Maple Mono NF CN | 7.9 | SIL OFL 1.1 | https://github.com/subframe7536/maple-font/releases/tag/v7.9 |
| Resource Han Rounded CJK glyph source | bundled through Maple Mono NF CN | SIL OFL 1.1 | https://github.com/CyanoHao/Resource-Han-Rounded |
| Nerd Fonts glyph set | 3.4.0 | Mixed; see the bundled license and license audit | https://github.com/ryanoasis/nerd-fonts/tree/v3.4.0 |

The Maple Mono faces are unmodified and retain their complete upstream CJK and
Nerd Font glyph set. The full Maple Mono OFL text, Nerd Fonts license, and Nerd
Fonts icon-source audit are shipped with desktop release artifacts and live in
`crates/ui/assets/fonts/` in the source tree.

The full Zeron distribution remains licensed under the terms in `LICENSE`.
