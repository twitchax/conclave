//! Conclave — the `conclave` binary.
//!
//! A thin CLI over `conclavelib`: parse args, initialise tracing, and dispatch to the library
//! (DESIGN.md §13). `serve` runs the central server, `bridge` the MCP+WS peer, and the control /
//! admin verbs are one-shot exchanges via [`conclavelib::control`]; `skill` prints or installs the
//! packaged Claude Code skill (the whole-CLI guide, generated help included).

#![feature(coverage_attribute)]

use std::{
    io::IsTerminal as _,
    path::{Path, PathBuf},
};

use anyhow::Context as _;
use clap::{Args, CommandFactory as _, Parser, Subcommand};
use conclavelib::{
    base::{PermissionLevel, Res, Visibility, Void},
    control,
    identity::{self, Identity, PermissionOverride, ServerRegistration},
    protocol::{AdminOp, ProtocolMessage},
    skill,
};
use tracing::error;

#[coverage(off)]
#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let directive = log_directive(cli.verbose, std::env::var("RUST_LOG").ok().as_deref());
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        // Colors only on a real terminal, so piped output / container logs stay clean (T-005).
        .with_ansi(std::io::stderr().is_terminal())
        .with_level(true)
        .with_target(false)
        .with_env_filter(tracing_subscriber::EnvFilter::new(directive))
        .init();

    if let Err(err) = execute(&cli).await {
        error!("❌ {err:#}");
        std::process::exit(1);
    }
}

/// The tracing filter directive: `RUST_LOG` if set and non-empty, else `debug` when `-v`, else
/// `info`. Keeping log level in the environment suits production / container deploys (PRD-0009 T-005).
fn log_directive(verbose: bool, rust_log: Option<&str>) -> String {
    match rust_log.filter(|value| !value.is_empty()) {
        Some(directive) => directive.to_owned(),
        None if verbose => "debug".to_owned(),
        None => "info".to_owned(),
    }
}

/// Dispatches a parsed command into `conclavelib`.
///
/// # Errors
///
/// Returns the subsystem's error, or a server-side rejection surfaced as an error frame.
async fn execute(cli: &Cli) -> Void {
    let dir = cli.config_dir.as_ref();
    match &cli.command {
        Command::Serve(args) => {
            // Refuse to silently run in-memory in production: a persistent store must be explicit
            // (--data-dir), and the ephemeral store must be opted into (PRD-0009 T-002).
            if args.data_dir.is_none() && !args.ephemeral {
                anyhow::bail!("`serve` requires `--data-dir <path>` for persistent storage (or `--ephemeral` for a throwaway in-memory store)");
            }
            conclavelib::server::serve(conclavelib::server::ServerConfig {
                bind: args.bind.clone(),
                data_dir: args.data_dir.clone(),
                admins: args.admins.iter().map(|spec| parse_admin_binding(spec)).collect(),
            })
            .await
        }
        Command::Bridge(args) => run_bridge(dir, args).await,
        Command::Key => run_key(dir),
        Command::Register(args) => run_register(dir, args).await,
        Command::Machine { command } => run_machine(dir, command).await,
        Command::Join(args) => run_join(dir, args).await,
        Command::Perm { command } => run_perm(dir, command),
        Command::Channel { command } => run_channel(dir, command).await,
        Command::Acl { command } => run_acl(dir, command).await,
        Command::Invite { command } => run_invite(dir, command).await,
        Command::Who(args) => print_response(control::one_shot(&args.server, &load_identity(dir)?, &cli_session(), ProtocolMessage::Who { channel: args.channel.clone() }).await?),
        Command::Kick(args) => {
            admin_op(
                dir,
                &args.server,
                AdminOp::Kick {
                    channel: args.channel.clone(),
                    target: args.target.clone(),
                },
            )
            .await
        }
        Command::Ban(args) => {
            admin_op(
                dir,
                &args.server,
                AdminOp::Ban {
                    channel: args.channel.clone(),
                    user: args.user.clone(),
                },
            )
            .await
        }
        Command::User { command } => run_user(dir, command).await,
        Command::Skill(args) => run_skill(args),
    }
}

// --- Shared helpers -----------------------------------------------------------

fn config_dir(explicit: Option<&PathBuf>) -> Res<PathBuf> {
    match explicit {
        Some(dir) => Ok(dir.clone()),
        None => identity::default_config_dir(),
    }
}

/// A short, per-process session handle for one-shot CLI ops (avoids colliding with a live bridge).
fn cli_session() -> String {
    format!("cli-{}", std::process::id())
}

fn load_identity(explicit: Option<&PathBuf>) -> Res<Identity> {
    identity::load_identity(&config_dir(explicit)?)
}

fn load_or_create_identity(dir: &Path) -> Res<Identity> {
    if dir.join("key").exists() {
        identity::load_identity(dir)
    } else {
        let identity = Identity::generate()?;
        identity::save_identity(dir, &identity)?;
        Ok(identity)
    }
}

/// Authenticates and sends `op` as an admin frame to `server`, printing the result.
async fn admin_op(explicit: Option<&PathBuf>, server: &str, op: AdminOp) -> Void {
    print_response(control::one_shot(server, &load_identity(explicit)?, &cli_session(), ProtocolMessage::Admin(op)).await?)
}

/// Prints a control response for a human; a server error frame becomes a non-zero exit.
fn print_response(response: ProtocolMessage) -> Void {
    match response {
        ProtocolMessage::Ack { detail } => println!("✓ {}", detail.unwrap_or_else(|| "ok".to_owned())),
        ProtocolMessage::Joined { channel } => println!("✓ joined {channel}"),
        ProtocolMessage::InviteToken { token } => println!("invite token: {token}"),
        ProtocolMessage::Established { path } => println!("✓ {path}"),
        ProtocolMessage::ChannelList { channels } => {
            for channel in channels {
                println!("{}\t{}{}", channel.name, channel.visibility.as_str(), if channel.member { "\t(member)" } else { "" });
            }
        }
        ProtocolMessage::MachineList { machines } => {
            for machine in machines {
                println!("{}\t{}\t{}", machine.name, machine.pubkey, machine.added_at);
            }
        }
        ProtocolMessage::UserList { users } => {
            for user in users {
                println!("{user}");
            }
        }
        ProtocolMessage::Presence { channel, sessions } => {
            let scope = channel.unwrap_or_else(|| "server".to_owned());
            let who = sessions.iter().map(std::string::ToString::to_string).collect::<Vec<_>>().join(", ");
            println!("[{scope}] {who}");
        }
        ProtocolMessage::Error(err) => anyhow::bail!("{err}"),
        other => anyhow::bail!("unexpected response: {other:?}"),
    }
    Ok(())
}

// --- Verb handlers ------------------------------------------------------------

fn run_key(explicit: Option<&PathBuf>) -> Void {
    let identity = load_or_create_identity(&config_dir(explicit)?)?;
    println!("{}", identity.public_key_base64());
    Ok(())
}

async fn run_register(explicit: Option<&PathBuf>, args: &RegisterArgs) -> Void {
    let dir = config_dir(explicit)?;
    let identity = load_or_create_identity(&dir)?;
    let machine = args.machine.clone().unwrap_or_else(default_machine_name);
    let path = control::register(&args.server, &identity, &args.username, &machine, &cli_session()).await?;

    let mut config = identity::load_config(&dir)?;
    config.servers.retain(|s| s.url != args.server);
    config.servers.push(ServerRegistration {
        url: args.server.clone(),
        username: args.username.clone(),
        machine,
    });
    identity::save_config(&dir, &config)?;

    println!("✓ registered {path}");
    Ok(())
}

async fn run_machine(explicit: Option<&PathBuf>, command: &MachineCommand) -> Void {
    match command {
        MachineCommand::Add { server, name, pubkey } => {
            let pubkey = identity::decode_key(pubkey).map_err(|e| anyhow::anyhow!("invalid public key: {e}"))?;
            admin_op(explicit, server, AdminOp::MachineAdd { name: name.clone(), pubkey }).await
        }
        MachineCommand::List { server } => print_response(control::one_shot(server, &load_identity(explicit)?, &cli_session(), ProtocolMessage::ListMachines).await?),
        MachineCommand::Remove { server, name } => admin_op(explicit, server, AdminOp::MachineRemove { name: name.clone() }).await,
    }
}

async fn run_join(explicit: Option<&PathBuf>, args: &JoinArgs) -> Void {
    let dir = config_dir(explicit)?;
    let identity = load_identity(explicit)?;

    // Parse the perm up front so a bad value fails before we touch the server.
    let perm = args.perm.as_deref().map(str::parse::<PermissionLevel>).transpose().map_err(anyhow::Error::from)?;

    let response = control::one_shot(
        &args.server,
        &identity,
        &cli_session(),
        ProtocolMessage::Join {
            channel: args.channel.clone(),
            token: args.token.clone(),
        },
    )
    .await?;
    // Bails on an `Error` frame — so a rejected join never reaches the persist below (#24).
    print_response(response)?;

    // Only now that the server has accepted the join do we persist the local permission override.
    if let Some(level) = perm {
        let mut config = identity::load_config(&dir)?;
        config.overrides.retain(|o| !(o.server == args.server && o.channel.as_deref() == Some(args.channel.as_str())));
        config.overrides.push(PermissionOverride {
            server: args.server.clone(),
            channel: Some(args.channel.clone()),
            level,
        });
        identity::save_config(&dir, &config)?;
    }

    eprintln!("note: verified access and set the local permission; your live session subscribes via the /conclave skill's join_channel tool.");
    Ok(())
}

fn run_perm(explicit: Option<&PathBuf>, command: &PermCommand) -> Void {
    let dir = config_dir(explicit)?;
    match command {
        PermCommand::Set { level, server, channel, whisper } => {
            let level: PermissionLevel = level.parse().map_err(anyhow::Error::from)?;
            let mut config = identity::load_config(&dir)?;
            if let Some(server) = server {
                // Require exactly one explicit scope so `--server` can't silently pick whisper (#25).
                let scope_channel = match (channel, *whisper) {
                    (Some(channel), false) => Some(channel.clone()),
                    (None, true) => None,
                    (None, false) => anyhow::bail!("--server needs an explicit scope: pass --channel <name> or --whisper"),
                    (Some(_), true) => anyhow::bail!("--channel and --whisper are mutually exclusive"),
                };
                config.overrides.retain(|o| !(o.server == *server && o.channel == scope_channel));
                config.overrides.push(PermissionOverride {
                    server: server.clone(),
                    channel: scope_channel,
                    level,
                });
            } else if channel.is_none() && !whisper {
                config.default_permission = level;
            } else {
                anyhow::bail!("--channel / --whisper require --server");
            }
            identity::save_config(&dir, &config)?;
            println!("✓ permission updated");
        }
        PermCommand::Show => {
            let config = identity::load_config(&dir)?;
            println!("default: {}", level_token(config.default_permission));
            for over in &config.overrides {
                let scope = over.channel.clone().unwrap_or_else(|| "<whisper>".to_owned());
                println!("{} {} -> {}", over.server, scope, level_token(over.level));
            }
        }
    }
    Ok(())
}

async fn run_channel(explicit: Option<&PathBuf>, command: &ChannelCommand) -> Void {
    match command {
        ChannelCommand::Create { server, name, visibility } => {
            let visibility = parse_visibility(visibility.as_deref())?;
            admin_op(explicit, server, AdminOp::CreateChannel { name: name.clone(), visibility }).await
        }
        ChannelCommand::Delete { server, name } => admin_op(explicit, server, AdminOp::DeleteChannel { name: name.clone() }).await,
        ChannelCommand::Rename { server, name, new_name } => {
            admin_op(
                explicit,
                server,
                AdminOp::RenameChannel {
                    name: name.clone(),
                    new_name: new_name.clone(),
                },
            )
            .await
        }
        ChannelCommand::SetVisibility { server, name, visibility } => {
            let visibility = visibility.parse().map_err(anyhow::Error::from)?;
            admin_op(explicit, server, AdminOp::SetVisibility { name: name.clone(), visibility }).await
        }
        ChannelCommand::List { server } => print_response(control::one_shot(server, &load_identity(explicit)?, &cli_session(), ProtocolMessage::ListChannels).await?),
    }
}

async fn run_acl(explicit: Option<&PathBuf>, command: &AclCommand) -> Void {
    match command {
        AclCommand::Add { server, channel, user } => {
            admin_op(
                explicit,
                server,
                AdminOp::AclAdd {
                    channel: channel.clone(),
                    user: user.clone(),
                },
            )
            .await
        }
        AclCommand::Remove { server, channel, user } => {
            admin_op(
                explicit,
                server,
                AdminOp::AclRemove {
                    channel: channel.clone(),
                    user: user.clone(),
                },
            )
            .await
        }
    }
}

async fn run_invite(explicit: Option<&PathBuf>, command: &InviteCommand) -> Void {
    match command {
        InviteCommand::Create { server, channel, uses, expires_in } => {
            let expires_in_secs = expires_in.as_deref().map(parse_duration_secs).transpose()?;
            admin_op(
                explicit,
                server,
                AdminOp::InviteCreate {
                    channel: channel.clone(),
                    uses: *uses,
                    expires_in_secs,
                },
            )
            .await
        }
        InviteCommand::Revoke { server, token } => admin_op(explicit, server, AdminOp::InviteRevoke { token: token.clone() }).await,
    }
}

async fn run_user(explicit: Option<&PathBuf>, command: &UserCommand) -> Void {
    match command {
        UserCommand::List { server } => print_response(control::one_shot(server, &load_identity(explicit)?, &cli_session(), ProtocolMessage::ListUsers).await?),
        UserCommand::Remove { server, username } => admin_op(explicit, server, AdminOp::UserRemove { username: username.clone() }).await,
    }
}

fn run_skill(args: &SkillArgs) -> Void {
    let content = skill::render(&render_command_reference());
    match &args.command {
        None | Some(SkillCommand::Show) => print!("{content}"),
        Some(SkillCommand::Install { dir }) => {
            let base = match dir {
                Some(dir) => dir.clone(),
                None => dirs::home_dir().context("could not determine the home directory")?.join(".claude").join("skills"),
            };
            let path = skill::install_path(&base);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).with_context(|| format!("failed to create `{}`", parent.display()))?;
            }
            std::fs::write(&path, content).with_context(|| format!("failed to write `{}`", path.display()))?;
            println!("✓ installed the conclave skill to {}", path.display());
        }
    }
    Ok(())
}

/// Loads the local identity + config and runs the bridge (MCP stdio peer + WS client).
async fn run_bridge(explicit: Option<&PathBuf>, args: &BridgeArgs) -> Void {
    let dir = config_dir(explicit)?;
    let identity = identity::load_identity(&dir)?;
    let config = identity::load_config(&dir)?;
    let session = match &args.session {
        Some(session) => session.clone(),
        None => identity::default_handle(&std::env::current_dir().context("failed to read the working directory")?),
    };
    conclavelib::bridge::run(conclavelib::bridge::BridgeSetup {
        identity,
        config,
        session,
        servers: args.servers.clone(),
    })
    .await
}

// --- Small parsers / renderers ------------------------------------------------

fn default_machine_name() -> String {
    gethostname::gethostname().to_string_lossy().into_owned()
}

fn level_token(level: PermissionLevel) -> String {
    format!("{level:?}").to_lowercase()
}

fn parse_visibility(value: Option<&str>) -> Res<Visibility> {
    match value {
        Some(value) => value.parse().map_err(anyhow::Error::from),
        None => Ok(Visibility::Public),
    }
}

/// Parses a human duration (`30s`, `10m`, `24h`, `7d`, or bare seconds) into seconds.
fn parse_duration_secs(value: &str) -> Res<u64> {
    let value = value.trim();
    let (digits, mult) = match value.chars().last() {
        Some('s') => (&value[..value.len() - 1], 1),
        Some('m') => (&value[..value.len() - 1], 60),
        Some('h') => (&value[..value.len() - 1], 3600),
        Some('d') => (&value[..value.len() - 1], 86_400),
        _ => (value, 1),
    };
    let count: u64 = digits.trim().parse().with_context(|| format!("invalid duration `{value}`"))?;
    Ok(count * mult)
}

/// Renders every subcommand's `--help` into the skill's command reference (always CLI-accurate).
fn render_command_reference() -> String {
    let mut out = String::new();
    append_command_help(&mut out, &Cli::command(), "conclave");
    out
}

fn append_command_help(out: &mut String, command: &clap::Command, path: &str) {
    use std::fmt::Write as _;

    let mut command = command.clone();
    let help = command.render_long_help().to_string();
    // Writing to a `String` is infallible.
    let _ = write!(out, "### `{path}`\n\n```\n{}\n```\n\n", help.trim_end());

    let subcommands: Vec<clap::Command> = command.get_subcommands().cloned().collect();
    for sub in &subcommands {
        // Skip clap's auto-generated `help` subcommand.
        if sub.get_name() != "help" {
            append_command_help(out, sub, &format!("{path} {}", sub.get_name()));
        }
    }
}

/// Discord-for-agents: shared channels that let Claude Code sessions talk to each other.
#[derive(Parser, Debug)]
#[command(name = "conclave", author, version, about, long_about = None, propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Config / keystore directory (defaults to `~/.config/conclave`).
    #[arg(long, global = true)]
    config_dir: Option<PathBuf>,

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
    /// Print or install the packaged Claude Code skill (the whole-CLI guide).
    Skill(SkillArgs),
}

#[cfg(test)]
impl Command {
    /// The top-level verb name (used by the CLI parse tests).
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
            Command::Skill(_) => "skill",
        }
    }
}

/// Parses a `--admin` spec: `user=<pubkey-b64>` pins the name to a key (anti-squat); a bare `user`
/// is unpinned (claimable first-come — warned at server startup). PRD-0007 T-002.
fn parse_admin_binding(spec: &str) -> (String, Option<String>) {
    match spec.split_once('=') {
        Some((user, pubkey)) => (user.to_owned(), Some(pubkey.to_owned())),
        None => (spec.to_owned(), None),
    }
}

#[derive(Args, Debug)]
struct ServeArgs {
    /// Address to bind the WSS endpoint to (env: `CONCLAVE_BIND`).
    #[arg(long, env = "CONCLAVE_BIND", default_value = "0.0.0.0:4390")]
    bind: String,
    /// Directory for the embedded database and server state, required unless `--ephemeral`
    /// (env: `CONCLAVE_DATA_DIR`).
    #[arg(long, env = "CONCLAVE_DATA_DIR")]
    data_dir: Option<PathBuf>,
    /// Run with a throwaway in-memory store instead of `--data-dir` (tests / experiments only; all
    /// state is lost on restart). Prevents a mis-templated deploy from silently running in-memory.
    #[arg(long, conflicts_with = "data_dir")]
    ephemeral: bool,
    /// Username granted server-wide admin as `user[=<pubkey-b64>]` (repeatable; comma-separated in
    /// the env var `CONCLAVE_ADMINS`). Pin the key to stop the name being squatted (DESIGN.md §7).
    #[arg(long = "admin", env = "CONCLAVE_ADMINS", value_delimiter = ',', value_name = "USER[=PUBKEY]")]
    admins: Vec<String>,
}

#[derive(Args, Debug)]
struct BridgeArgs {
    /// Server URL to connect to (repeatable); defaults to the known-servers list.
    #[arg(long = "server")]
    servers: Vec<String>,
    /// Session handle for this connection; defaults to the working-directory name.
    #[arg(long = "as")]
    session: Option<String>,
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

#[derive(Args, Debug)]
struct SkillArgs {
    #[command(subcommand)]
    command: Option<SkillCommand>,
}

#[derive(Subcommand, Debug)]
enum SkillCommand {
    /// Print the SKILL.md to stdout (the default).
    Show,
    /// Install the skill under the Claude Code skills directory (`/conclave` becomes available).
    Install {
        /// Skills directory (defaults to `~/.claude/skills`).
        #[arg(long)]
        dir: Option<PathBuf>,
    },
}

#[cfg(test)]
mod tests {
    // Tests relax `unwrap_used` (house convention; DESIGN.md §22).
    #![allow(clippy::unwrap_used)]

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
    fn perm_scope_with_server_requires_an_unambiguous_scope() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();

        // `--server` with neither scope is ambiguous — previously it silently wrote the whisper scope.
        let ambiguous = PermCommand::Set {
            level: "converse".to_owned(),
            server: Some("wss://s1".to_owned()),
            channel: None,
            whisper: false,
        };
        let err = run_perm(Some(&dir), &ambiguous).expect_err("--server with no scope must be rejected");
        assert!(err.to_string().contains("explicit scope"), "{err}");

        // `--channel` and `--whisper` together are contradictory.
        let conflicting = PermCommand::Set {
            level: "converse".to_owned(),
            server: Some("wss://s1".to_owned()),
            channel: Some("ops".to_owned()),
            whisper: true,
        };
        let err = run_perm(Some(&dir), &conflicting).expect_err("--channel + --whisper must be rejected");
        assert!(err.to_string().contains("mutually exclusive"), "{err}");

        // An explicit whisper scope is accepted and persisted.
        let explicit = PermCommand::Set {
            level: "converse".to_owned(),
            server: Some("wss://s1".to_owned()),
            channel: None,
            whisper: true,
        };
        run_perm(Some(&dir), &explicit).expect("an explicit whisper scope is accepted");
        let config = identity::load_config(&dir).unwrap();
        assert_eq!(config.overrides.len(), 1);
        assert_eq!(config.overrides[0].channel, None);
    }

    #[test]
    fn verbose_is_a_global_flag() {
        let cli = Cli::parse_from(["conclave", "-v", "key"]);
        assert!(cli.verbose);
        assert_eq!(cli.command.verb(), "key");
    }

    #[test]
    fn log_config_directive_precedence() {
        assert_eq!(log_directive(false, None), "info");
        assert_eq!(log_directive(true, None), "debug");
        // RUST_LOG overrides the -v / default; an empty RUST_LOG is ignored.
        assert_eq!(log_directive(false, Some("warn")), "warn");
        assert_eq!(log_directive(true, Some("trace")), "trace");
        assert_eq!(log_directive(true, Some("")), "debug");
    }

    #[test]
    fn config_dir_is_global_and_parses_after_the_subcommand() {
        let cli = Cli::parse_from(["conclave", "register", "--server", "wss://s", "--username", "aaron", "--config-dir", "/tmp/x"]);
        assert_eq!(cli.config_dir.as_deref(), Some(std::path::Path::new("/tmp/x")));
        assert_eq!(cli.command.verb(), "register");
    }

    #[test]
    fn skill_subcommand_parses_show_and_install() {
        assert_eq!(Cli::parse_from(["conclave", "skill"]).command.verb(), "skill");
        let install = Cli::parse_from(["conclave", "skill", "install", "--dir", "/tmp/skills"]);
        match install.command {
            Command::Skill(args) => assert!(matches!(args.command, Some(SkillCommand::Install { .. }))),
            other => panic!("expected `skill`, parsed {other:?}"),
        }
    }
}
