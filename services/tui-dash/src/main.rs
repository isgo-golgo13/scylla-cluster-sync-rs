// services/tui-dash/src/main.rs
//
// TUI Dashboard for scylla-cluster-sync-rs
// Terminal-based monitoring for SSTable migrations
//
// Run with: cargo run --bin tui-dash -- --demo

use std::io::stdout;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{
    prelude::*,
    widgets::*,
};

mod state;
mod mock;
mod api;

use state::DashboardState;
use mock::MockDataGenerator;
use api::ApiClient;

#[derive(Parser, Debug)]
#[command(name = "tui-dash")]
#[command(about = "Terminal UI Dashboard for scylla-cluster-sync-rs migrations")]
#[command(version = "0.1.0")]
struct Args {
    /// Run in demo mode with simulated data (no DB connection required)
    #[arg(long, short)]
    demo: bool,

    /// SSTable-loader API endpoint
    #[arg(long, default_value = "http://localhost:9092")]
    api_url: String,

    /// Refresh interval in milliseconds
    #[arg(long, default_value = "100")]
    refresh_ms: u64,
}

// Color palette: Red, White, Silver, Gold
mod colors {
    use ratatui::style::Color;
    
    pub const RED: Color = Color::Rgb(220, 50, 47);
    pub const DARK_RED: Color = Color::Rgb(139, 0, 0);
    pub const WHITE: Color = Color::Rgb(253, 246, 227);
    pub const SILVER: Color = Color::Rgb(147, 161, 161);
    pub const GOLD: Color = Color::Rgb(255, 193, 37);
    pub const DARK_GOLD: Color = Color::Rgb(184, 134, 11);
    pub const BG_DARK: Color = Color::Rgb(0, 20, 30);
    pub const BG_PANEL: Color = Color::Rgb(7, 30, 41);
    pub const SUCCESS: Color = Color::Rgb(133, 153, 0);
    pub const ERROR: Color = Color::Rgb(220, 50, 47);
}

fn main() -> Result<()> {
    let args = Args::parse();
    
    // Setup terminal
    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;
    
    // Run app
    let result = run_app(&mut terminal, args);
    
    // Restore terminal
    disable_raw_mode()?;
    stdout().execute(LeaveAlternateScreen)?;
    
    result
}

fn run_app<B: Backend>(terminal: &mut Terminal<B>, args: Args) -> Result<()> {
    let mut state = DashboardState::new();
    let mut mock_gen = MockDataGenerator::new();
    let mut api_client = if !args.demo {
        Some(ApiClient::new(&args.api_url))
    } else {
        None
    };
    
    let tick_rate = Duration::from_millis(args.refresh_ms);
    let mut last_tick = Instant::now();
    
    // Initial log
    if args.demo {
        state.add_log("INFO", "TUI Dashboard started in DEMO mode");
    } else {
        state.add_log("INFO", &format!("TUI Dashboard started - connecting to {}", args.api_url));
    }
    
    loop {
        // Draw UI
        terminal.draw(|frame| draw_ui(frame, &state, args.demo, api_client.as_ref()))?;
        
        // Handle input
        let timeout = tick_rate.saturating_sub(last_tick.elapsed());
        if event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                        KeyCode::Char(' ') => state.toggle_pause(),
                        KeyCode::Char('r') => state.reset(),
                        KeyCode::Up => state.scroll_up(),
                        KeyCode::Down => state.scroll_down(),
                        _ => {}
                    }
                }
            }
        }
        
        // Update state
        if last_tick.elapsed() >= tick_rate {
            if args.demo {
                mock_gen.update(&mut state);
            } else if let Some(ref mut client) = api_client {
                client.fetch_status(&mut state);
            }
            last_tick = Instant::now();
        }
    }
}

fn draw_ui(frame: &mut Frame, state: &DashboardState, demo_mode: bool, api_client: Option<&ApiClient>) {
    let area = frame.area();
    
    // Background
    frame.render_widget(
        Block::default().style(Style::default().bg(colors::BG_DARK)),
        area,
    );
    
    // Main layout
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Header
            Constraint::Length(7),  // Stats panel
            Constraint::Length(5),  // Progress
            Constraint::Min(10),    // Tables + Activity
            Constraint::Length(3),  // Footer
        ])
        .split(area);
    
    draw_header(frame, chunks[0], state, demo_mode, api_client);
    draw_stats_panel(frame, chunks[1], state);
    draw_progress(frame, chunks[2], state);
    draw_main_content(frame, chunks[3], state);
    draw_footer(frame, chunks[4]);
}

fn draw_header(frame: &mut Frame, area: Rect, state: &DashboardState, demo_mode: bool, api_client: Option<&ApiClient>) {
    let status_indicator = if state.is_running {
        if state.is_paused { "[PAUSED]" } else { "[RUNNING]" }
    } else {
        "[STOPPED]"
    };
    
    let status_color = if state.is_running {
        if state.is_paused { colors::GOLD } else { colors::SUCCESS }
    } else {
        colors::SILVER
    };
    
    // Mode indicator (DEMO vs LIVE)
    let (mode_text, mode_color) = if demo_mode {
        ("DEMO", colors::GOLD)
    } else if let Some(client) = api_client {
        if client.is_connected() {
            ("LIVE", colors::SUCCESS)
        } else {
            ("DISCONNECTED", colors::RED)
        }
    } else {
        ("LIVE", colors::SILVER)
    };
    
    let title = Line::from(vec![
        Span::styled(
            " SCYLLA-CLUSTER-SYNC ",
            Style::default().fg(colors::WHITE).bg(colors::DARK_RED).bold(),
        ),
        Span::raw("  "),
        Span::styled(
            "TUI DASHBOARD",
            Style::default().fg(colors::GOLD).bold(),
        ),
        Span::raw("  "),
        Span::styled(
            format!("[{}]", mode_text),
            Style::default().fg(mode_color).bold(),
        ),
        Span::raw("  "),
        Span::styled(
            status_indicator,
            Style::default().fg(status_color).bold(),
        ),
        Span::raw("  "),
        Span::styled(
            &state.elapsed_display,
            Style::default().fg(colors::SILVER),
        ),
    ]);
    
    let header = Paragraph::new(title)
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(Style::default().fg(colors::DARK_RED))
                .style(Style::default().bg(colors::BG_DARK)),
        );
    
    frame.render_widget(header, area);
}

fn draw_stats_panel(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
        ])
        .split(area);
    
    // Total Rows
    draw_stat_box(
        frame,
        chunks[0],
        "TOTAL ROWS",
        &format_number(state.total_rows),
        colors::WHITE,
    );
    
    // Migrated Rows
    draw_stat_box(
        frame,
        chunks[1],
        "MIGRATED",
        &format_number(state.migrated_rows),
        colors::SUCCESS,
    );
    
    // Failed Rows
    draw_stat_box(
        frame,
        chunks[2],
        "FAILED",
        &format_number(state.failed_rows),
        if state.failed_rows > 0 { colors::RED } else { colors::SILVER },
    );
    
    // Throughput
    draw_stat_box(
        frame,
        chunks[3],
        "THROUGHPUT",
        &format!("{:.0} rows/sec", state.throughput),
        colors::GOLD,
    );
}

fn draw_stat_box(frame: &mut Frame, area: Rect, label: &str, value: &str, value_color: Color) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::SILVER))
        .border_type(BorderType::Rounded)
        .style(Style::default().bg(colors::BG_PANEL));
    
    let inner = block.inner(area);
    frame.render_widget(block, area);
    
    let text = vec![
        Line::from(Span::styled(
            label,
            Style::default().fg(colors::SILVER).add_modifier(Modifier::DIM),
        )),
        Line::from(""),
        Line::from(Span::styled(
            value,
            Style::default().fg(value_color).bold().add_modifier(Modifier::BOLD),
        )),
    ];
    
    let paragraph = Paragraph::new(text).alignment(Alignment::Center);
    frame.render_widget(paragraph, inner);
}

fn draw_progress(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let block = Block::default()
        .title(Span::styled(
            " MIGRATION PROGRESS ",
            Style::default().fg(colors::GOLD).bold(),
        ))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::DARK_GOLD))
        .border_type(BorderType::Rounded)
        .style(Style::default().bg(colors::BG_PANEL));
    
    let inner = block.inner(area);
    frame.render_widget(block, area);
    
    let progress_chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([Constraint::Length(1), Constraint::Length(1)])
        .split(inner);
    
    // Progress bar
    let _progress = (state.progress_percent / 100.0).clamp(0.0, 1.0);
    let gauge = Gauge::default()
        .gauge_style(
            Style::default()
                .fg(colors::GOLD)
                .bg(colors::BG_DARK),
        )
        .percent((state.progress_percent as u16).min(100))
        .label(format!("{:.2}%", state.progress_percent));
    
    frame.render_widget(gauge, progress_chunks[0]);
    
    // Tables progress
    let tables_info = Line::from(vec![
        Span::styled("Tables: ", Style::default().fg(colors::SILVER)),
        Span::styled(
            format!("{}/{}", state.tables_completed, state.tables_total),
            Style::default().fg(colors::WHITE).bold(),
        ),
        Span::raw("  |  "),
        Span::styled("Skipped: ", Style::default().fg(colors::SILVER)),
        Span::styled(
            format!("{}", state.tables_skipped),
            Style::default().fg(if state.tables_skipped > 0 { colors::RED } else { colors::SILVER }),
        ),
        Span::raw("  |  "),
        Span::styled("Ranges: ", Style::default().fg(colors::SILVER)),
        Span::styled(
            format!("{}/{}", state.ranges_completed, state.ranges_total),
            Style::default().fg(colors::WHITE),
        ),
    ]);
    
    frame.render_widget(Paragraph::new(tables_info).alignment(Alignment::Center), progress_chunks[1]);
}

fn draw_main_content(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);
    
    draw_tables_panel(frame, chunks[0], state);
    draw_activity_panel(frame, chunks[1], state);
}

fn draw_tables_panel(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let block = Block::default()
        .title(Span::styled(
            " TABLES ",
            Style::default().fg(colors::WHITE).bold(),
        ))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::SILVER))
        .border_type(BorderType::Rounded)
        .style(Style::default().bg(colors::BG_PANEL));
    
    let rows: Vec<Row> = state
        .tables
        .iter()
        .map(|t| {
            let status_style = match t.status.as_str() {
                "DONE" => Style::default().fg(colors::SUCCESS),
                "RUNNING" => Style::default().fg(colors::GOLD),
                "FAILED" => Style::default().fg(colors::RED),
                _ => Style::default().fg(colors::SILVER),
            };
            
            Row::new(vec![
                Cell::from(Span::styled(&t.status, status_style)),
                Cell::from(Span::styled(&t.name, Style::default().fg(colors::WHITE))),
                Cell::from(Span::styled(
                    format!("{:.1}%", t.progress),
                    Style::default().fg(colors::SILVER),
                )),
            ])
        })
        .collect();
    
    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Min(20),
            Constraint::Length(8),
        ],
    )
    .header(
        Row::new(vec![
            Cell::from(Span::styled("STATUS", Style::default().fg(colors::GOLD).bold())),
            Cell::from(Span::styled("TABLE NAME", Style::default().fg(colors::GOLD).bold())),
            Cell::from(Span::styled("PROGRESS", Style::default().fg(colors::GOLD).bold())),
        ])
        .bottom_margin(1),
    )
    .block(block)
    .row_highlight_style(Style::default().bg(colors::BG_DARK));
    
    frame.render_widget(table, area);
}

fn draw_activity_panel(frame: &mut Frame, area: Rect, state: &DashboardState) {
    let block = Block::default()
        .title(Span::styled(
            " ACTIVITY LOG ",
            Style::default().fg(colors::WHITE).bold(),
        ))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(colors::SILVER))
        .border_type(BorderType::Rounded)
        .style(Style::default().bg(colors::BG_PANEL));
    
    let logs: Vec<Line> = state
        .activity_log
        .iter()
        .rev()
        .take(20)
        .map(|entry| {
            let (prefix, color) = match entry.level.as_str() {
                "ERROR" => ("[ERR]", colors::RED),
                "WARN" => ("[WRN]", colors::GOLD),
                "INFO" => ("[INF]", colors::SUCCESS),
                _ => ("[---]", colors::SILVER),
            };
            
            Line::from(vec![
                Span::styled(
                    format!("{} ", entry.timestamp.format("%H:%M:%S")),
                    Style::default().fg(colors::SILVER).add_modifier(Modifier::DIM),
                ),
                Span::styled(format!("{} ", prefix), Style::default().fg(color)),
                Span::styled(&entry.message, Style::default().fg(colors::WHITE)),
            ])
        })
        .collect();
    
    let paragraph = Paragraph::new(logs)
        .block(block)
        .wrap(Wrap { trim: true });
    
    frame.render_widget(paragraph, area);
}

fn draw_footer(frame: &mut Frame, area: Rect) {
    let help = Line::from(vec![
        Span::styled(" [Q] ", Style::default().fg(colors::BG_DARK).bg(colors::RED)),
        Span::styled(" Quit ", Style::default().fg(colors::SILVER)),
        Span::raw("  "),
        Span::styled(" [SPACE] ", Style::default().fg(colors::BG_DARK).bg(colors::GOLD)),
        Span::styled(" Pause/Resume ", Style::default().fg(colors::SILVER)),
        Span::raw("  "),
        Span::styled(" [R] ", Style::default().fg(colors::BG_DARK).bg(colors::WHITE)),
        Span::styled(" Reset ", Style::default().fg(colors::SILVER)),
        Span::raw("  "),
        Span::styled(" [UP/DOWN] ", Style::default().fg(colors::BG_DARK).bg(colors::SILVER)),
        Span::styled(" Scroll ", Style::default().fg(colors::SILVER)),
    ]);
    
    let footer = Paragraph::new(help)
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::TOP)
                .border_style(Style::default().fg(colors::DARK_RED))
                .style(Style::default().bg(colors::BG_DARK)),
        );
    
    frame.render_widget(footer, area);
}

fn format_number(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.2}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.2}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.2}K", n as f64 / 1_000.0)
    } else {
        format!("{}", n)
    }
}
