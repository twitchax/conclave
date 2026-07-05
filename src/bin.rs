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

    // One-shot CLI verbs behave like any Unix CLI in a pipeline: die quietly on SIGPIPE instead of
    // panicking on BrokenPipe (Rust ignores SIGPIPE by default). The long-running verbs keep the
    // ignore disposition — serve/bridge handle write errors on their graceful shutdown paths.
    if !matches!(cli.command, Command::Serve(_) | Command::Bridge(_)) {
        restore_default_sigpipe();
    }

    let telemetry = init_telemetry(&cli);

    let result = execute(&cli).await;
    // Flush any buffered OTLP spans and log records before the process ends (PRD-0014 T-002).
    if let Some(providers) = telemetry {
        let _ = providers.tracer.shutdown();
        let _ = providers.logger.shutdown();
    }
    if let Err(err) = result {
        error!("❌ {err:#}");
        std::process::exit(1);
    }
}

/// The OTLP providers `main` must flush on shutdown: spans (PRD-0014) and logs (PRD-0017)
/// share the endpoint gate and lifecycle but buffer independently.
struct OtlpProviders {
    tracer: opentelemetry_sdk::trace::SdkTracerProvider,
    logger: opentelemetry_sdk::logs::SdkLoggerProvider,
}

/// Initializes telemetry (PRD-0014): a stderr `fmt` layer — human-pretty, or JSON lines via
/// `CONCLAVE_LOG_FORMAT=json` for platform log pipelines — plus, for `serve` only, OTLP trace
/// and log exporters when `CONCLAVE_OTLP_ENDPOINT` is set (no endpoint → no exporter task at
/// all). Returns the providers so `main` can flush them on shutdown.
fn init_telemetry(cli: &Cli) -> Option<OtlpProviders> {
    use tracing_subscriber::{Layer as _, layer::SubscriberExt as _, util::SubscriberInitExt as _};

    let directive = log_directive(cli.verbose, std::env::var("RUST_LOG").ok().as_deref());
    let filter = tracing_subscriber::EnvFilter::new(directive);
    let json = std::env::var("CONCLAVE_LOG_FORMAT").is_ok_and(|format| format.eq_ignore_ascii_case("json"));

    let providers = if matches!(cli.command, Command::Serve(_)) {
        std::env::var("CONCLAVE_OTLP_ENDPOINT")
            .ok()
            .filter(|endpoint| !endpoint.is_empty())
            .and_then(|endpoint| match otlp_providers(&endpoint) {
                Ok(providers) => Some(providers),
                // A broken exporter must not take the server down — log and serve without it.
                Err(err) => {
                    eprintln!("⚠ CONCLAVE_OTLP_ENDPOINT is set but the exporter failed to build: {err:#}");
                    None
                }
            })
    } else {
        None
    };
    let otel_layer = providers.as_ref().map(|providers| {
        use opentelemetry::trace::TracerProvider as _;
        tracing_opentelemetry::layer().with_tracer(providers.tracer.tracer("conclave"))
    });
    // Events become OTLP log records too (PRD-0017 T-001) — minus the exporter stack's own
    // targets, or every export POST would emit records that trigger the next export (feedback).
    let log_bridge = providers.as_ref().map(|providers| {
        opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&providers.logger).with_filter(tracing_subscriber::filter::filter_fn(|meta| {
            let target = meta.target();
            !["opentelemetry", "reqwest", "hyper", "h2"].iter().any(|noise| target.starts_with(noise))
        }))
    });

    let registry = tracing_subscriber::registry().with(filter).with(otel_layer).with(log_bridge);
    if json {
        registry.with(tracing_subscriber::fmt::layer().json().with_writer(std::io::stderr).with_target(false)).init();
    } else {
        // Colors only on a real terminal, so piped output / container logs stay clean (T-005).
        registry
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_ansi(std::io::stderr().is_terminal())
                    .with_target(false),
            )
            .init();
    }
    providers
}

/// Builds the OTLP/HTTP span and log exporter pipelines for `serve` (PRD-0014 T-002,
/// PRD-0017 T-001). The env var is the collector *base* URL (e.g. `http://localhost:4318`),
/// matching `OTEL_EXPORTER_OTLP_ENDPOINT` semantics — the signal paths are appended here
/// because an explicit exporter endpoint is otherwise used verbatim.
fn otlp_providers(endpoint: &str) -> Res<OtlpProviders> {
    use opentelemetry_otlp::WithExportConfig as _;

    let base = endpoint.trim_end_matches('/');
    let resource = opentelemetry_sdk::Resource::builder()
        .with_service_name("conclave")
        .with_attribute(opentelemetry::KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .build();

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(format!("{base}/v1/traces"))
        .build()
        .context("failed to build the OTLP span exporter")?;
    let tracer = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource.clone())
        .build();

    let log_exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_endpoint(format!("{base}/v1/logs"))
        .build()
        .context("failed to build the OTLP log exporter")?;
    let logger = opentelemetry_sdk::logs::SdkLoggerProvider::builder().with_batch_exporter(log_exporter).with_resource(resource).build();

    Ok(OtlpProviders { tracer, logger })
}

/// Restores the default SIGPIPE disposition (terminate) so pipeline writes end the process quietly.
#[cfg(unix)]
fn restore_default_sigpipe() {
    // SAFETY: `signal(2)` with `SIG_DFL` has no preconditions; called once at startup before any
    // I/O the disposition could affect.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
}

#[cfg(not(unix))]
fn restore_default_sigpipe() {}

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
        Command::Server { command } => run_server(dir, command),
        Command::Join(args) => run_join(dir, args).await,
        Command::Perm { command } => run_perm(dir, command),
        Command::Channel { command } => run_channel(dir, command).await,
        Command::Acl { command } => run_acl(dir, command).await,
        Command::Invite { command } => run_invite(dir, command).await,
        Command::Status => run_status(dir).await,
        Command::Send(args) => {
            control::send_message(&args.server, &load_identity(dir)?, &cli_session(), &args.channel, &args.text).await?;
            println!("✓ sent to {}", args.channel);
            Ok(())
        }
        Command::Tail(args) => {
            let since_secs = args.since.as_deref().map(conclavelib::base::parse_duration_secs).transpose()?;
            control::tail(&args.server, &load_identity(dir)?, &args.session.clone().unwrap_or_else(cli_session), &args.channel, since_secs).await
        }
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
        Command::Unban(args) => {
            admin_op(
                dir,
                &args.server,
                AdminOp::Unban {
                    channel: args.channel.clone(),
                    user: args.user.clone(),
                },
            )
            .await
        }
        Command::Bans(args) => admin_op(dir, &args.server, AdminOp::BanList { channel: args.channel.clone() }).await,
        Command::User { command } => run_user(dir, command).await,
        Command::Skill(args) => run_skill(args),
        Command::Completions { shell } => {
            clap_complete::generate(*shell, &mut Cli::command(), "conclave", &mut std::io::stdout());
            Ok(())
        }
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
        ProtocolMessage::InviteList { invites } => {
            for invite in invites {
                let uses = invite.uses_remaining.map_or_else(|| "unlimited".to_owned(), |u| u.to_string());
                let expires = invite.expires_at.unwrap_or_else(|| "never".to_owned());
                println!("{}\tuses: {uses}\texpires: {expires}", invite.token);
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

/// Handles `conclave server …`: local known-servers management (PRD-0012 T-004). Purely local —
/// nothing here talks to a server; `remove` is the CLI exit for a stranded registration (the
/// state behind the two-URLs-one-server supersede storm).
fn run_server(explicit: Option<&PathBuf>, command: &ServerCommand) -> Void {
    let dir = config_dir(explicit)?;
    match command {
        ServerCommand::List => {
            let config = identity::load_config(&dir)?;
            if config.servers.is_empty() {
                println!("no servers registered (see `conclave register`)");
            }
            for registration in &config.servers {
                println!("{}\t{}/{}", registration.url, registration.username, registration.machine);
            }
        }
        ServerCommand::Remove { url } => {
            let mut config = identity::load_config(&dir)?;
            let servers_before = config.servers.len();
            config.servers.retain(|r| r.url != *url);
            anyhow::ensure!(config.servers.len() < servers_before, "no registration for `{url}` (see `conclave server list`)");
            let overrides_before = config.overrides.len();
            config.overrides.retain(|o| o.server != *url);
            identity::save_config(&dir, &config)?;
            println!("✓ forgot `{url}` ({} permission override(s) removed)", overrides_before - config.overrides.len());
        }
    }
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
            println!("✓ permission updated (applies to newly started bridges; a live session changes levels with its `set_perm` tool)");
        }
        PermCommand::Show => {
            let config = identity::load_config(&dir)?;
            print_perm_table(&config);
        }
    }
    Ok(())
}

/// Prints the resolved permission table (shared by `perm show` and `status`).
fn print_perm_table(config: &identity::Config) {
    println!("default: {}", level_token(config.default_permission));
    for over in &config.overrides {
        let scope = over.channel.clone().unwrap_or_else(|| "<whisper>".to_owned());
        println!("{} {} -> {}", over.server, scope, level_token(over.level));
    }
}

/// The `status` verb: registrations, per-server reachability probes, and the permission table.
async fn run_status(explicit: Option<&PathBuf>) -> Void {
    let dir = config_dir(explicit)?;
    let config = identity::load_config(&dir)?;
    if config.servers.is_empty() {
        println!("no servers registered; run `conclave register --server <url> --username <you>`");
        return Ok(());
    }

    let identity = load_identity(explicit)?;
    let mut unreachable = 0_usize;
    for reg in &config.servers {
        // A `who` round-trip proves reachability AND that this key authenticates (bounded by the
        // control-client timeouts), and yields the live-session count as a bonus.
        match control::one_shot(&reg.url, &identity, &cli_session(), ProtocolMessage::Who { channel: None }).await {
            Ok(ProtocolMessage::Presence { sessions, .. }) => {
                println!("{}\t{}/{}\treachable ({} session(s) online)", reg.url, reg.username, reg.machine, sessions.len());
            }
            Ok(other) => {
                println!("{}\t{}/{}\tunreachable: unexpected response {other:?}", reg.url, reg.username, reg.machine);
                unreachable += 1;
            }
            Err(err) => {
                println!("{}\t{}/{}\tunreachable: {err:#}", reg.url, reg.username, reg.machine);
                unreachable += 1;
            }
        }
    }

    println!();
    print_perm_table(&config);

    if unreachable > 0 {
        anyhow::bail!("{unreachable} server(s) unreachable");
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
        AclCommand::List { server, channel } => admin_op(explicit, server, AdminOp::AclList { channel: channel.clone() }).await,
    }
}

async fn run_invite(explicit: Option<&PathBuf>, command: &InviteCommand) -> Void {
    match command {
        InviteCommand::Create { server, channel, uses, expires_in } => {
            let expires_in_secs = expires_in.as_deref().map(conclavelib::base::parse_duration_secs).transpose()?;
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
        InviteCommand::List { server, channel } => admin_op(explicit, server, AdminOp::InviteList { channel: channel.clone() }).await,
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
        // An explicit `--as` pins the name; a defaulted handle self-disambiguates on collision
        // (PRD-0018) — same-directory sessions are the normal case, not the anomaly.
        Some(session) => conclavelib::bridge::SessionHandle::explicit(session.clone()),
        None => conclavelib::bridge::SessionHandle::defaulted(identity::default_handle(&std::env::current_dir().context("failed to read the working directory")?)),
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
    /// Manage this machine's server registrations (the local known-servers list).
    Server {
        #[command(subcommand)]
        command: ServerCommand,
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
    /// Create, list, or revoke channel invite tokens.
    Invite {
        #[command(subcommand)]
        command: InviteCommand,
    },
    /// Show this machine's registrations, server reachability, and the permission table.
    Status,
    /// Post one message to a channel from the command line.
    Send(SendArgs),
    /// Stream a channel's traffic to the terminal until Ctrl-C.
    Tail(TailArgs),
    /// List presence on a server or within a channel.
    Who(WhoArgs),
    /// Kick a live session or user from a channel.
    Kick(KickArgs),
    /// Ban a user from a channel.
    Ban(BanArgs),
    /// Lift a channel ban (does not grant ACL membership).
    Unban(BanArgs),
    /// List a channel's banned users.
    Bans(BansArgs),
    /// Server-admin user management: list, remove.
    User {
        #[command(subcommand)]
        command: UserCommand,
    },
    /// Print or install the packaged Claude Code skill (the whole-CLI guide).
    Skill(SkillArgs),
    /// Generate shell completions (bash, zsh, fish, elvish, powershell).
    Completions {
        /// The shell to generate completions for.
        shell: clap_complete::Shell,
    },
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
            Command::Server { .. } => "server",
            Command::Join(_) => "join",
            Command::Perm { .. } => "perm",
            Command::Channel { .. } => "channel",
            Command::Acl { .. } => "acl",
            Command::Invite { .. } => "invite",
            Command::Status => "status",
            Command::Send(_) => "send",
            Command::Tail(_) => "tail",
            Command::Who(_) => "who",
            Command::Kick(_) => "kick",
            Command::Ban(_) => "ban",
            Command::Unban(_) => "unban",
            Command::Bans(_) => "bans",
            Command::User { .. } => "user",
            Command::Skill(_) => "skill",
            Command::Completions { .. } => "completions",
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
enum ServerCommand {
    /// List the servers this machine is registered on.
    List,
    /// Forget a server registration and its permission overrides — local only; the account and
    /// machine enrollment on the server itself are untouched.
    Remove {
        /// The server URL to forget.
        url: String,
    },
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
        /// The new machine's public key (base64url), as printed by `conclave key` on that machine.
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
    /// List the users on a channel's access-control list (channel-admin).
    List {
        /// Server the channel is on.
        #[arg(long)]
        server: String,
        /// Channel to list.
        #[arg(long)]
        channel: String,
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
    /// List a channel's outstanding invite tokens (channel-admin).
    List {
        /// Server the channel is on.
        #[arg(long)]
        server: String,
        /// Channel to list invites for.
        #[arg(long)]
        channel: String,
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

/// Arguments for `send` (post one message from the command line).
#[derive(Args, Debug)]
struct SendArgs {
    /// Server hosting the channel.
    #[arg(long)]
    server: String,
    /// Channel to post to.
    #[arg(long)]
    channel: String,
    /// The message text.
    text: String,
}

/// Arguments for `tail` (stream a channel to the terminal).
#[derive(Args, Debug)]
struct TailArgs {
    /// Server hosting the channel.
    #[arg(long)]
    server: String,
    /// Channel to stream.
    #[arg(long)]
    channel: String,
    /// Session handle for the tail connection (defaults to a per-process handle).
    #[arg(long = "as")]
    session: Option<String>,
    /// Replay the retained backlog this far back first (e.g. `2h`, `45m`, `1d`; up to 7 days).
    #[arg(long)]
    since: Option<String>,
}

/// Arguments for `bans` (list a channel's banned users).
#[derive(Args, Debug)]
struct BansArgs {
    /// Server the channel is on.
    #[arg(long)]
    server: String,
    /// Channel to list bans for.
    #[arg(long)]
    channel: String,
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
