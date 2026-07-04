pub mod chat_protobufs {
    tonic::include_proto!("chat");
}

use chat_protobufs::chat_service_client::ChatServiceClient;
use chat_protobufs::{ChatMessage, GetMessagesRequest};
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, List, ListItem, Paragraph},
};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

struct ConnectionStatus {
    connected: bool,
    last_error: Option<String>,
}

struct AppState {
    message_buffer: Arc<Mutex<VecDeque<ChatMessage>>>,
    max_messages: usize,
    status: Arc<Mutex<ConnectionStatus>>,
    fetch_trigger: mpsc::Sender<()>,
}

fn save_client_buffer(buf: &VecDeque<ChatMessage>) -> Result<(), std::io::Error> {
    let serialized = serde_json::to_string(buf)?;
    std::fs::write("client_state.json", serialized)?;
    Ok(())
}

fn load_client_buffer() -> VecDeque<ChatMessage> {
    if let Ok(content) = std::fs::read_to_string("client_state.json") {
        if let Ok(buf) = serde_json::from_str::<VecDeque<ChatMessage>>(&content) {
            return buf;
        }
    }
    VecDeque::new()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let mut n = 10;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-n" | "--limit" => {
                if i + 1 < args.len() {
                    if let Ok(parsed) = args[i + 1].parse::<usize>() {
                        n = parsed;
                    } else {
                        eprintln!("Error: Invalid value for -n/--limit. Expected integer.");
                        std::process::exit(1);
                    }
                    i += 2;
                } else {
                    eprintln!("Error: Missing value for -n/--limit.");
                    std::process::exit(1);
                }
            }
            val => {
                if let Ok(parsed) = val.parse::<usize>() {
                    n = parsed;
                } else if val == "-h" || val == "--help" {
                    println!("Usage: client [-n <limit>]");
                    std::process::exit(0);
                } else {
                    eprintln!("Error: Unknown argument '{}'.", val);
                    std::process::exit(1);
                }
                i += 1;
            }
        }
    }

    let max_buffer = 3 * n;
    println!(
        "[Client] Connecting to Stream Chat Server at http://[::1]:50051... (Displaying N={}, pre-fetching max={})",
        n, max_buffer
    );

    let buffer = Arc::new(Mutex::new(load_client_buffer()));
    let status = Arc::new(Mutex::new(ConnectionStatus {
        connected: false,
        last_error: None,
    }));
    let (trigger_tx, mut trigger_rx) = mpsc::channel::<()>(10);

    // Setup TUI
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Spawn background task to keep buffer filled
    let buffer_clone = Arc::clone(&buffer);
    let status_clone = Arc::clone(&status);

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = trigger_rx.recv() => {}
            }

            let len = {
                let buf = buffer_clone.lock().unwrap();
                buf.len()
            };

            if len <= 2 * n {
                match ChatServiceClient::connect("http://[::1]:50051").await {
                    Ok(mut client) => {
                        {
                            let mut s = status_clone.lock().unwrap();
                            s.connected = true;
                            s.last_error = None;
                        }

                        let request = tonic::Request::new(GetMessagesRequest { limit: n as u32 });

                        match client.get_messages(request).await {
                            Ok(response) => {
                                let new_msgs = response.into_inner().messages;
                                if !new_msgs.is_empty() {
                                    let mut buf = buffer_clone.lock().unwrap();
                                    // Make sure we don't exceed 3N
                                    let space = max_buffer.saturating_sub(buf.len());
                                    let to_add = std::cmp::min(space, new_msgs.len());
                                    buf.extend(new_msgs.into_iter().take(to_add));
                                    if let Err(e) = save_client_buffer(&buf) {
                                        eprintln!("[Client] Error saving buffer: {:?}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                let mut s = status_clone.lock().unwrap();
                                s.last_error = Some(format!("gRPC Call Error: {}", e));
                            }
                        }
                    }
                    Err(e) => {
                        let mut s = status_clone.lock().unwrap();
                        s.connected = false;
                        s.last_error = Some(format!("Connection failed: {}", e));
                    }
                }
            }
        }
    });

    let mut state = AppState {
        message_buffer: buffer,
        status,
        fetch_trigger: trigger_tx,
        max_messages: n,
    };

    // Main event and rendering loop
    let res = run_app(&mut terminal, &mut state).await;

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        println!("[Client Error] {:?}", err);
    }

    Ok(())
}

async fn run_app<B: ratatui::backend::Backend>(
    terminal: &mut Terminal<B>,
    state: &mut AppState,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        terminal.draw(|f| ui(f, state))?;

        // Non-blocking poll for events to allow UI updates and tick count decreases
        if event::poll(Duration::from_millis(50))? {
            match event::read()? {
                Event::Key(key) => match key.code {
                    KeyCode::Char('q') | KeyCode::Char('Q') => {
                        return Ok(());
                    }
                    KeyCode::Char(' ') | KeyCode::Char('a') | KeyCode::Char('A') => {
                        acknowledge_message(state).await;
                    }
                    _ => {}
                },
                _ => {}
            }
        }
    }
}

async fn acknowledge_message(state: &mut AppState) {
    let limit = state.max_messages;
    let len = {
        let mut buf = state.message_buffer.lock().unwrap();
        buf.pop_front();
        if let Err(e) = save_client_buffer(&buf) {
            eprintln!("[Client] Error saving buffer: {:?}", e);
        }
        buf.len()
    };
    if len <= 2 * limit {
        let _ = state.fetch_trigger.try_send(());
    }
}

fn ui(f: &mut ratatui::Frame, state: &mut AppState) {
    let size = f.size();
    let limit = state.max_messages;
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(limit as u16), Constraint::Length(1)])
        .split(size);

    let buffer_msgs = state.message_buffer.lock().unwrap().clone();
    let mut list_items = Vec::new();
    let display_count = std::cmp::min(limit, buffer_msgs.len());

    for i in (0..display_count).rev() {
        let msg = &buffer_msgs[i];
        let platform_tag = if msg.platform == "YouTube" {
            Span::styled(
                " [YouTube] ",
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            )
        } else {
            Span::styled(
                " [Twitch]  ",
                Style::default()
                    .fg(Color::Magenta)
                    .add_modifier(Modifier::BOLD),
            )
        };

        let sender = Span::styled(
            format!("{}: ", msg.sender),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        );

        let content = Span::styled(&msg.content, Style::default().fg(Color::Gray));

        let (marker, style) = match i {
            0 => (
                Span::styled(
                    "▶",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Style::default().bg(Color::Rgb(40, 40, 20)),
            ),
            _ => (Span::raw(" "), Style::default()),
        };

        list_items.push(
            ListItem::new(Line::from(vec![marker, platform_tag, sender, content])).style(style),
        );
    }

    let chat_list = List::new(list_items).block(Block::default());
    f.render_widget(chat_list, chunks[0]);

    // Status Block
    let (connected, last_err) = {
        let s = state.status.lock().unwrap();
        (s.connected, s.last_error.clone())
    };
    let color = match connected {
        true => Color::Green,
        false => Color::Red,
    };
    let conn_status = Span::styled("◉", Style::default().fg(color).add_modifier(Modifier::BOLD));
    let err_msg = match last_err {
        Some(e) => format!(" | Error: {} ", e),
        None => " ".to_string(),
    };

    let status_text = Line::from(vec![
        conn_status,
        Span::styled(err_msg, Style::default().fg(Color::Red)),
    ]);

    let status_block = Paragraph::new(status_text)
        .block(Block::default())
        .alignment(Alignment::Right);
    f.render_widget(status_block, chunks[1]);
}
