# PostOffice Style Guide

Engineering **blueprint** drawing with a **TRON neon-blue** accent. Shares tokens with the sibling `synth` and `bil` sites. Source of truth: `design/tokens.css`.

## Palette

| Token | Dark | Light | Use |
|---|---|---|---|
| `--bg` | `#070b14` | `#f3f7fc` | page background |
| `--surface` | `#0d1424` | `#ffffff` | cards, title block fill |
| `--line` | `#1e2a44` | `#d3deec` | grid, hairlines |
| `--line-strong` | `#2c3d63` | `#9fb6d3` | border ticks, dividers |
| `--text` | `#e6edf7` | `#0b1730` | body text |
| `--muted` | `#8a9ab5` | `#4a5d7a` | secondary text, grid refs |
| `--cyan` | `#38bdf8` | `#0a7fb8` | dimension lines, centerlines, links |
| `--indigo` | `#818cf8` | `#4f46e5` | rare secondary accent |
| `--neon` | `#00e5ff` | `#0086a8` | primary strokes, headings, glow |

Logos, icons and drawings (SVG) are authored on dark and always carry their own dark plate where needed.

## Typography

- Mono (headings, labels, drawing text): `ui-monospace, "SF Mono", Menlo, Consolas, monospace`, bold, wide letter-spacing (`.18em`-`.24em` for eyebrows/wordmark).
- Sans (long body copy): system stack. No webfonts, no external requests.
- Drawing text is UPPERCASE mono, 9-13px; grid refs 12px muted.

## Grid

64px square grid, 1px `--line`, offset -1px (`.bp-grid`). Drawings use a 40px minor grid. Border: outer frame `--line-strong` 2px, inner frame neon 1.5px, tick marks every 100 units with column letters A.. (top/bottom) and row numbers 1.. (left/right).

## Glow recipe

SVG: `<filter id="g"><feGaussianBlur stdDeviation="3" result="b"/><feMerge><feMergeNode in="b"/><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge></filter>` applied to neon strokes only (not text labels < 14px).
CSS: `text-shadow: 0 0 6px var(--neon-glow), 0 0 18px var(--neon-glow)` or `filter: drop-shadow(0 0 6px var(--neon-glow))` (`.glow`).

## Corner brackets

Cards use 14px, 2px neon L-shaped brackets at top-left and bottom-right (`.bp-card`). Logo mark uses four 10px `--line-strong` brackets inside its plate.

## Dimension lines

- Stroke `--cyan` 1px; solid filled arrowheads (10px long, 6px wide); extension lines overshoot 8-10px.
- Label centered on the line, 12px mono, on a bg-colored knockout rect.
- Centerlines: `stroke-dasharray="14 4 3 4"` (dash-dot), `--cyan`, 60% opacity.
- Callouts: 12px-radius circle, bg fill, neon 1.5px stroke, number inside, leader line to a 2.5px dot.
- Hatch: 45-degree lines, 2px `--line-strong`, 8px pitch, for sections/engine regions.

## Title block

Bottom-right of the frame, 380x110 (hero) in `--surface` with neon 1.5px outline and `--line-strong` cell dividers. Rows: `PROJECT: POSTOFFICE | DWG NO: PO-001`; `TITLE: MULTI-PROTOCOL MESSAGE BRIDGE`; `REV: vX.Y.Z | SCALE: 1:1 | SHEET: 1/1`. Cell captions 9px muted, values 13px text. Bump REV on each release.

## Icons

64x64 viewBox, stroke `#00e5ff`, 2px stroke, `fill="none"`, round caps/joins, 1.5px allowed for secondary details only. No fills except bg-colored knobs. Metaphors: mqtt=pub/sub hub, amqp=queue slots, kafka=partition log, nanomq=edge chip, router=fork, control-plane=sliders, data-plane=parallel flow, observability=trace chart.

## Usage

- Favicon: `assets/logo-mark.svg`. README header: `assets/logo.svg` (520px wide). Social/OG: `assets/social-card.svg` (1280x640; some crawlers need PNG, export if so).
- Keep one neon element per focal area; everything else in `--line-strong`/`--muted`.
- Pages: `<link rel="stylesheet" href="design/tokens.css">`; theme switch via `data-theme="light|dark"` on `<html>`.
- Every SVG needs `<title>`, `viewBox`, no scripts, no external fonts.
