//! The packaged Claude Code skill — the CLI is the single source of truth (DESIGN.md §13).
//!
//! `conclave skill` emits a complete `SKILL.md`: a curated guide (the mental model, the two
//! surfaces, one-time install, examples) followed by an auto-generated command reference so the
//! flag list never drifts from the actual CLI. `conclave skill install` writes it under the Claude
//! Code skills directory so `/conclave` becomes available. The binary generates the reference (clap
//! lives in the binary) and passes it to [`render`]; this module owns the curated body and paths.

use std::path::{Path, PathBuf};

/// The curated skill body (frontmatter + prose). The command reference is appended by [`render`].
const SKILL_BODY: &str = include_str!("skill/SKILL.md");

/// Renders the full `SKILL.md`: the curated guide plus the generated command reference.
#[must_use]
pub fn render(command_reference: &str) -> String {
    format!(
        "{}\n\n## Command reference\n\nAuto-generated from `conclave --help` — the authoritative, always-current flag list for every verb.\n\n{}\n",
        SKILL_BODY.trim_end(),
        command_reference.trim_end(),
    )
}

/// The install path for the skill under `skills_dir` (`<skills_dir>/conclave/SKILL.md`).
#[must_use]
pub fn install_path(skills_dir: &Path) -> PathBuf {
    skills_dir.join("conclave").join("SKILL.md")
}

#[cfg(test)]
mod tests {
    // Tests relax `unwrap_used` (house convention; DESIGN.md §22).
    #![allow(clippy::unwrap_used)]

    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn skill_render_has_frontmatter_and_appends_the_reference() {
        let rendered = render("### conclave register\n```\n--username ...\n```");
        // Valid skill frontmatter at the very top.
        assert!(rendered.starts_with("---\n"), "skill must start with YAML frontmatter: {}", &rendered[..40]);
        assert!(rendered.contains("name: conclave"));
        // The join section (the headline action) and the generated reference are both present.
        assert!(rendered.contains("join_channel"), "skill must document joining via the bridge tool");
        assert!(rendered.contains("## Command reference"));
        assert!(rendered.contains("conclave register"));
    }

    #[test]
    fn skill_install_path_targets_the_conclave_skill_dir() {
        assert_eq!(install_path(Path::new("/home/x/.claude/skills")), Path::new("/home/x/.claude/skills/conclave/SKILL.md"));
    }
}
