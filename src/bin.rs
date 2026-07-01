//! Conclave — the `conclave` binary.
//!
//! A thin CLI over `conclavelib`: parse args, initialise tracing, and dispatch to the library.
//! The full command surface (DESIGN.md §13) is declared here in M0; the verbs behind it land in
//! M1–M5, so today every command parses and then reports that it is not yet implemented.

#![feature(coverage_attribute)]

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use conclavelib::base::Void;
use tracing::error;

#[coverage(off)]
#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let level = if cli.verbose { tracing::Level::DEBUG } else { tracing::Level::INFO };
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(true)
        .with_level(true)
        .with_target(false)
        .with_max_level(level)
        .init();

    if let Err(err) = execute(&cli.command) {
        error!("❌ {err:#}");
        std::process::exit(1);
    }
}

/// Dispatch a parsed command. In M0 the surface exists but the verbs do not, so this reports
/// the command as unimplemented rather than silently succeeding.
///
/// # Errors
///
/// Always returns an error in M0 (the requested command is not yet implemented). From M1 on,
/// each arm routes into `conclavelib` and surfaces that subsystem's errors.
fn execute(command: &Command) -> Void {
    anyhow::bail!("`conclave {}` is not yet implemented (M0 scaffold — see .prds/)", command.verb());
}

/// Discord-for-agents: shared channels that let Claude Code sessions talk to each other.
#[derive(Parser, Debug)]
#[command(name = "conclave", author, version, about, long_about = None, propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Increase logging verbosity to debug level.
    #[arg(short, long, global = true)]
    verbose: bool,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the central server: WSS endpoint, identity store, presence, and fan-out.
    Serve(ServeArgs),
    /// Run the local bridge: an MCP server to Claude Code plus a WS client to servers.
    Bridge(BridgeArgs),
    /// Generate this machine's keypair and print its public key.
    Key,
    /// Claim a username on a server and enroll this machine as its first key.
    Register(RegisterArgs),
    /// Manage the machines (authorized keys) enrolled under your user.
    Machine {
        #[command(subcommand)]
        command: MachineCommand,
    },
    /// Join a channel on a server and subscribe this session to it.
    Join(JoinArgs),
    /// Inspect or set local per-channel autonomy (permission) levels.
    Perm {
        #[command(subcommand)]
        command: PermCommand,
    },
    /// Administer channels: create, delete, rename, set visibility, list.
    Channel {
        #[command(subcommand)]
        command: ChannelCommand,
    },
    /// Administer a channel's access-control list.
    Acl {
        #[command(subcommand)]
        command: AclCommand,
    },
    /// Create or revoke channel invite tokens.
    Invite {
        #[command(subcommand)]
        command: InviteCommand,
    },
    /// List presence on a server or within a channel.
    Who(WhoArgs),
    /// Kick a live session or user from a channel.
    Kick(KickArgs),
    /// Ban a user from a channel.
    Ban(BanArgs),
    /// Server-admin user management: list, remove.
    User {
        #[command(subcommand)]
        command: UserCommand,
    },
}

impl Command {
    /// The top-level verb, used for diagnostics until the arms are implemented.
    fn verb(&self) -> &'static str {
        match self {
            Command::Serve(_) => "serve",
            Command::Bridge(_) => "bridge",
            Command::Key => "key",
            Command::Register(_) => "register",
            Command::Machine { .. } => "machine",
            Command::Join(_) => "join",
            Command::Perm { .. } => "perm",
            Command::Channel { .. } => "channel",
            Command::Acl { .. } => "acl",
            Command::Invite { .. } => "invite",
            Command::Who(_) => "who",
            Command::Kick(_) => "kick",
            Command::Ban(_) => "ban",
            Command::User { .. } => "user",
        }
    }
}

#[derive(Args, Debug)]
struct ServeArgs {
    /// Address to bind the WSS endpoint to.
    #[arg(long, default_value = "0.0.0.0:4390")]
    bind: String,
    /// Directory for the embedded database and server state.
    #[arg(long)]
    data_dir: Option<PathBuf>,
}

#[derive(Args, Debug)]
struct BridgeArgs {
    /// Server URL to connect to (repeatable); defaults to the known-servers list.
    #[arg(long = "server")]
    servers: Vec<String>,
}

#[derive(Args, Debug)]
struct RegisterArgs {
    /// Server to register on.
    #[arg(long)]
    server: String,
    /// Username to claim (unique per server).
    #[arg(long)]
    username: String,
    /// Machine name for this key (defaults to the hostname).
    #[arg(long)]
    machine: Option<String>,
}

#[derive(Subcommand, Debug)]
enum MachineCommand {
    /// Authorize a new machine's public key under your user.
    Add {
        /// Server the machine is enrolled on.
        #[arg(long)]
        server: String,
        /// Unique name for the machine within your user.
        #[arg(long)]
        name: String,
        /// The new machine's public key (PEM), from `conclave key`.
        #[arg(long)]
        pubkey: String,
    },
    /// List the machines enrolled under your user.
    List {
        /// Server to query.
        #[arg(long)]
        server: String,
    },
    /// Revoke a machine, force-dropping its live sessions.
    Remove {
        /// Server the machine is enrolled on.
        #[arg(long)]
        server: String,
        /// Name of the machine to revoke.
        name: String,
    },
}

#[derive(Args, Debug)]
struct JoinArgs {
    /// Server hosting the channel.
    #[arg(long)]
    server: String,
    /// Channel name to join.
    channel: String,
    /// Invite token, if the channel requires one.
    #[arg(long)]
    token: Option<String>,
    /// Autonomy level for this channel: mute, notify, converse, or act.
    #[arg(long)]
    perm: Option<String>,
}

#[derive(Subcommand, Debug)]
enum PermCommand {
    /// Set the autonomy level for a channel, the whisper scope, or the machine default.
    Set {
        /// Level to set: mute, notify, converse, or act.
        level: String,
        /// Scope the level to a single server.
        #[arg(long)]
        server: Option<String>,
        /// Scope the level to a single channel.
        #[arg(long)]
        channel: Option<String>,
        /// Apply the level to the whisper scope instead of a channel.
        #[arg(long)]
        whisper: bool,
    },
    /// Print the resolved permission table.
    Show,
}

#[derive(Subcommand, Debug)]
enum ChannelCommand {
    /// Create a channel.
    Create {
        /// Server to create the channel on.
        #[arg(long)]
        server: String,
        /// Channel name.
        name: String,
        /// Visibility: public, unlisted, or private.
        #[arg(long)]
        visibility: Option<String>,
    },
    /// Delete a channel.
    Delete {
        /// Server the channel is on.
        #[arg(long)]
        server: String,
        /// Channel name.
        name: String,
    },
    /// Rename a channel.
    Rename {
        /// Server the channel is on.
        #[arg(long)]
        server: String,
        /// Current channel name.
        name: String,
        /// New channel name.
        new_name: String,
    },
    /// Change a channel's visibility.
    SetVisibility {
        /// Server the channel is on.
        #[arg(long)]
        server: String,
        /// Channel name.
        name: String,
        /// Visibility: public, unlisted, or private.
        visibility: String,
    },
    /// List channels visible to you on a server.
    List {
        /// Server to query.
        #[arg(long)]
        server: String,
    },
}

#[derive(Subcommand, Debug)]
enum AclCommand {
    /// Add a user to a channel's access-control list.
    Add {
        /// Server the channel is on.
        #[arg(long)]
        server: String,
        /// Channel to modify.
        #[arg(long)]
        channel: String,
        /// Username to add.
        user: String,
    },
    /// Remove a user from a channel's access-control list.
    Remove {
        /// Server the channel is on.
        #[arg(long)]
        server: String,
        /// Channel to modify.
        #[arg(long)]
        channel: String,
        /// Username to remove.
        user: String,
    },
}

#[derive(Subcommand, Debug)]
enum InviteCommand {
    /// Create an invite token for a channel.
    Create {
        /// Server the channel is on.
        #[arg(long)]
        server: String,
        /// Channel to invite to.
        #[arg(long)]
        channel: String,
        /// Maximum number of redemptions (unlimited if omitted).
        #[arg(long)]
        uses: Option<u32>,
        /// Lifetime before the token expires, e.g. 24h (never if omitted).
        #[arg(long)]
        expires_in: Option<String>,
    },
    /// Revoke an invite token.
    Revoke {
        /// Server the token is on.
        #[arg(long)]
        server: String,
        /// The token to revoke.
        token: String,
    },
}

#[derive(Args, Debug)]
struct WhoArgs {
    /// Server to query.
    #[arg(long)]
    server: String,
    /// Restrict presence to a single channel.
    channel: Option<String>,
}

#[derive(Args, Debug)]
struct KickArgs {
    /// Server the channel is on.
    #[arg(long)]
    server: String,
    /// Channel to kick from.
    #[arg(long)]
    channel: String,
    /// Session path or username to kick.
    target: String,
}

#[derive(Args, Debug)]
struct BanArgs {
    /// Server the channel is on.
    #[arg(long)]
    server: String,
    /// Channel to ban from.
    #[arg(long)]
    channel: String,
    /// Username to ban.
    user: String,
}

#[derive(Subcommand, Debug)]
enum UserCommand {
    /// List users registered on a server.
    List {
        /// Server to query.
        #[arg(long)]
        server: String,
    },
    /// Remove a user from a server.
    Remove {
        /// Server to remove the user from.
        #[arg(long)]
        server: String,
        /// Username to remove.
        username: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use pretty_assertions::assert_eq;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn help_lists_the_core_subcommands() {
        let help = Cli::command().render_long_help().to_string();
        for verb in ["serve", "bridge", "register", "machine", "join", "perm", "key"] {
            assert!(help.contains(verb), "help output is missing the `{verb}` subcommand");
        }
    }

    #[test]
    fn serve_parses_its_bind_flag() {
        let cli = Cli::parse_from(["conclave", "serve", "--bind", "127.0.0.1:9000"]);
        match cli.command {
            Command::Serve(args) => assert_eq!(args.bind, "127.0.0.1:9000"),
            other => panic!("expected `serve`, parsed {other:?}"),
        }
    }

    #[test]
    fn verbose_is_a_global_flag() {
        let cli = Cli::parse_from(["conclave", "-v", "key"]);
        assert!(cli.verbose);
        assert_eq!(cli.command.verb(), "key");
    }
}
