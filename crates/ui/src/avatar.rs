//! Deterministic blobatar-style avatars: a seed string (email, scope label)
//! hashes to a blob silhouette, hue gradient, and eyes, emitted as an
//! in-memory SVG and drawn through gpui's image pipeline. `Image` is keyed by
//! a content hash, so each distinct avatar rasterizes once per session.

use gpui::{App, EntityId, Image, ImageFormat, Img, Styled, img, px};
use std::sync::Arc;

/// Square avatar for `seed`, `size` logical pixels on a side. Same seed,
/// same face — across frames, restarts, and devices.
pub fn blob_avatar(seed: &str, size: f32) -> Img {
    blob_frame(seed, size, 1.0)
}

/// [`blob_avatar`] that blinks every few seconds. Renders are timer-driven —
/// one wake per blink keyframe, one across the rest of the cycle — and the
/// openness values come from a fixed three-level table, so each seed only
/// ever rasterizes two frames beyond the resting one it shares with the
/// static avatar. Honors reduced motion.
pub fn blob_avatar_live(seed: &str, size: f32, view: EntityId, cx: &mut App) -> Img {
    // The phase offset reuses the seed's first trait draw: any stable value
    // in [0, 1) works, and a fresh Traits stream leaves the face untouched.
    let openness = crate::motion::blink_openness(Traits::new(seed).unit(), view, cx);
    blob_frame(seed, size, openness)
}

fn blob_frame(seed: &str, size: f32, openness: f32) -> Img {
    // gpui rasterizes SVGs at their intrinsic size; author at 2× the logical
    // size so 1x and 2x displays both sample down, never up.
    let dim = (size * 2.0).round() as u32;
    img(Arc::new(Image::from_bytes(
        ImageFormat::Svg,
        svg_frame(seed, dim, openness).into_bytes(),
    )))
    .size(px(size))
    .flex_none()
}

/// Square organization badge for `seed`, `size` logical pixels on a side. Same palette
/// and pipeline as [`blob_avatar`], but a mirror-symmetric straight-edged crest
/// with no eyes: geometry reads as "Organization", the organic face reads as "person".
pub fn organization_avatar(seed: &str, size: f32) -> Img {
    let dim = (size * 2.0).round() as u32;
    img(Arc::new(Image::from_bytes(
        ImageFormat::Svg,
        organization_svg(seed, dim).into_bytes(),
    )))
    .size(px(size))
    .flex_none()
}

/// xorshift64 seeded from the FNV-1a hash of the seed string: one stable
/// stream of unit floats, consumed trait by trait. Reordering draws would
/// reshuffle every existing avatar, so only append new traits at the end.
struct Traits(u64);

impl Traits {
    fn new(seed: &str) -> Self {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in seed.as_bytes() {
            h ^= u64::from(*byte);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        // xorshift has a fixed point at zero; nudge it off.
        Self(if h == 0 { 0x9e37_79b9_7f4a_7c15 } else { h })
    }

    /// Next value in [0, 1).
    fn unit(&mut self) -> f32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 40) as f32 / (1u64 << 24) as f32
    }

    /// Next value in [lo, hi).
    fn range(&mut self, lo: f32, hi: f32) -> f32 {
        lo + (hi - lo) * self.unit()
    }
}

fn hex(color: gpui::Hsla) -> String {
    let rgba = gpui::Rgba::from(color);
    format!(
        "#{:02x}{:02x}{:02x}",
        (rgba.r * 255.0).round() as u8,
        (rgba.g * 255.0).round() as u8,
        (rgba.b * 255.0).round() as u8
    )
}

/// The seed's theme color: the top stop of the avatar gradient. Reproduces
/// the FIRST draw from the trait stream — `gradient` is the stream's first
/// consumer — so it matches [`blob_avatar`]'s hue for the same seed, forever.
pub fn seed_color(seed: &str) -> gpui::Hsla {
    gpui::hsla(Traits::new(seed).unit(), 0.62, 0.66, 1.0)
}

/// Two-stop vertical gradient, hue per seed. Saturation/lightness are pinned to
/// a band that reads on both the light and dark themes. Consumes exactly one
/// value from the stream, so both avatar kinds stay hue-compatible.
fn gradient(t: &mut Traits) -> (String, String) {
    let hue = t.unit();
    (
        hex(gpui::hsla(hue, 0.62, 0.66, 1.0)),
        hex(gpui::hsla((hue + 0.09) % 1.0, 0.58, 0.52, 1.0)),
    )
}

/// One frame of the blob face. `openness` scales the eyes' vertical radius:
/// 1.0 is the resting face (round eyes), lower values are mid-blink lids.
fn svg_frame(seed: &str, dim: u32, openness: f32) -> String {
    let mut t = Traits::new(seed);
    let (top, bottom) = gradient(&mut t);

    // Silhouette: 8 spokes with jittered radii around (64, 64) in a 128
    // viewBox, joined by a closed Catmull-Rom spline emitted as cubics.
    const N: usize = 8;
    let phase = t.range(0.0, std::f32::consts::TAU);
    let pts: Vec<(f32, f32)> = (0..N)
        .map(|i| {
            let a = phase + i as f32 / N as f32 * std::f32::consts::TAU;
            let r = 52.0 * t.range(0.86, 1.0);
            (64.0 + r * a.cos(), 64.0 + r * a.sin())
        })
        .collect();
    let mut d = format!("M{:.1} {:.1}", pts[0].0, pts[0].1);
    for i in 0..N {
        let p0 = pts[(i + N - 1) % N];
        let p1 = pts[i];
        let p2 = pts[(i + 1) % N];
        let p3 = pts[(i + 2) % N];
        d.push_str(&format!(
            "C{:.1} {:.1} {:.1} {:.1} {:.1} {:.1}",
            p1.0 + (p2.0 - p0.0) / 6.0,
            p1.1 + (p2.1 - p0.1) / 6.0,
            p2.0 - (p3.0 - p1.0) / 6.0,
            p2.1 - (p3.1 - p1.1) / 6.0,
            p2.0,
            p2.1
        ));
    }
    d.push('Z');

    // Eyes: upper-middle of the blob, spacing/height/size per seed. The
    // tightest silhouette keeps ~44px of interior radius, so these always
    // land well inside the fill.
    let eye_dx = t.range(10.0, 15.0);
    let eye_y = t.range(52.0, 60.0);
    let eye_r = t.range(4.2, 5.8);

    // Blinking squashes the eye vertically; the resting face keeps the exact
    // circle markup it has always had, so its content hash — and cached
    // rasterization — never changes.
    let eye = |x: f32| {
        if openness >= 1.0 {
            format!(r##"<circle cx="{x:.1}" cy="{eye_y:.1}" r="{eye_r:.1}" fill="#22242a"/>"##)
        } else {
            let ry = (eye_r * openness).max(0.8);
            format!(
                r##"<ellipse cx="{x:.1}" cy="{eye_y:.1}" rx="{eye_r:.1}" ry="{ry:.1}" fill="#22242a"/>"##
            )
        }
    };
    let eyes = format!("{}{}", eye(64.0 - eye_dx), eye(64.0 + eye_dx));

    format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="{dim}" height="{dim}" viewBox="0 0 128 128"><defs><linearGradient id="g" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="{top}"/><stop offset="1" stop-color="{bottom}"/></linearGradient></defs><path d="{d}" fill="url(#g)"/>{eyes}</svg>"##
    )
}

fn organization_svg(seed: &str, dim: u32) -> String {
    let mut t = Traits::new(seed);
    let (top, bottom) = gradient(&mut t);

    // Crest: 8 spokes from a top vertex, joined by straight edges. Symmetry
    // and hard corners are what separate an Organization from a person at 16px; the
    // eyes are deliberately absent. Only the right half is generated — the
    // left is its exact reflection, snapped to whole viewBox units so the
    // emitted digits mirror too, not just the floats behind them.
    const N: usize = 8;
    let right: Vec<(f32, f32)> = (0..=N / 2)
        .map(|i| {
            let a = -std::f32::consts::FRAC_PI_2 + i as f32 / N as f32 * std::f32::consts::TAU;
            let r = 54.0 * t.range(0.80, 1.0);
            ((64.0 + r * a.cos()).round(), (64.0 + r * a.sin()).round())
        })
        .collect();
    let d = right
        .iter()
        .copied()
        .chain(right[1..N / 2].iter().rev().map(|&(x, y)| (128.0 - x, y)))
        .enumerate()
        .map(|(i, (x, y))| format!("{}{x:.0} {y:.0}", if i == 0 { "M" } else { "L" }))
        .collect::<String>()
        + "Z";

    // The same outline at half scale, as a light wash: without it two flat
    // shapes of the same hue only differ by their silhouette.
    format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="{dim}" height="{dim}" viewBox="0 0 128 128"><defs><linearGradient id="g" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="{top}"/><stop offset="1" stop-color="{bottom}"/></linearGradient></defs><path d="{d}" fill="url(#g)"/><path d="{d}" fill="#ffffff" fill-opacity="0.14" transform="translate(64 64) scale(0.55) translate(-64 -64)"/></svg>"##
    )
}

#[cfg(test)]
mod tests {
    use super::{hex, organization_svg, seed_color, svg_frame};

    fn svg(seed: &str, dim: u32) -> String {
        svg_frame(seed, dim, 1.0)
    }

    #[test]
    fn seed_color_matches_avatar_gradient_top_stop() {
        for seed in ["a@b.c", "", "agent-chat-1", "组织"] {
            let s = svg(seed, 56);
            let top = s
                .split("stop-color=\"")
                .nth(1)
                .and_then(|rest| rest.split('"').next())
                .unwrap_or_else(|| panic!("no stop-color: {s}"));
            assert_eq!(hex(seed_color(seed)), top, "seed {seed:?}");
        }
    }

    #[test]
    fn deterministic_distinct_and_well_formed() {
        assert_eq!(svg("a@b.c", 56), svg("a@b.c", 56));
        assert_ne!(svg("a@b.c", 56), svg("x@y.z", 56));
        for seed in ["a@b.c", "", "Local only", "Stored on this device"] {
            let s = svg(seed, 56);
            assert!(s.starts_with("<svg"), "not an svg: {s}");
            assert!(!s.contains("NaN") && !s.contains("inf"), "bad number: {s}");
        }
    }

    #[test]
    fn blink_frames_share_the_face_and_only_squash_the_eyes() {
        for openness in [0.5, 0.15] {
            let open = svg("a@b.c", 56);
            let lidded = svg_frame("a@b.c", 56, openness);
            assert_ne!(open, lidded);
            assert!(lidded.contains("<ellipse"), "no lids: {lidded}");
            assert!(!lidded.contains("NaN") && !lidded.contains("inf"));
            // Same silhouette: only the eye markup differs.
            let silhouette = |s: &str| {
                s.split(" d=\"")
                    .nth(1)
                    .and_then(|rest| rest.split('"').next())
                    .map(String::from)
            };
            assert_eq!(silhouette(&open), silhouette(&lidded), "silhouette moved");
        }
    }

    #[test]
    fn organization_badges_are_stable_symmetric_and_not_faces() {
        assert_eq!(organization_svg("org-1", 56), organization_svg("org-1", 56));
        assert_ne!(organization_svg("org-1", 56), organization_svg("org-2", 56));
        // An Organization never wears a person's face, even on a colliding seed.
        assert_ne!(organization_svg("org-1", 56), svg("org-1", 56));

        // Regression anchors against computing both halves instead of
        // reflecting one: `sym-3390` rounds to an asymmetric pair of digits,
        // `snap-4302` stays asymmetric even with both halves snapped to whole
        // units.
        for seed in [
            "org-1",
            "",
            "org-with-a-very-long-identifier",
            "sym-3390",
            "snap-4302",
            "组织",
        ] {
            let s = organization_svg(seed, 56);
            assert!(s.starts_with("<svg"), "not an svg: {s}");
            assert!(!s.contains("NaN") && !s.contains("inf"), "bad number: {s}");
            assert!(!s.contains("<circle"), "organization badge grew eyes: {s}");

            // Mirror check: every vertex has a partner at the same y, its x
            // reflected through the 64 axis to the digit (the top and bottom
            // vertices lie on the axis and pair with themselves).
            let d = s
                .split(" d=\"")
                .nth(1)
                .and_then(|rest| rest.split('"').next())
                .unwrap_or_else(|| panic!("no path data: {s}"));
            let pts: Vec<(f32, f32)> = d
                .split(['M', 'L', 'Z'])
                .filter_map(|seg| {
                    let (x, y) = seg.split_once(' ')?;
                    Some((x.parse().ok()?, y.parse().ok()?))
                })
                .collect();
            assert_eq!(pts.len(), 8, "expected 8 vertices: {s}");
            for (x, y) in &pts {
                assert!(
                    pts.iter().any(|(mx, my)| *mx == 128.0 - x && my == y),
                    "vertex ({x}, {y}) has no mirror: {s}"
                );
            }
        }
    }
}
