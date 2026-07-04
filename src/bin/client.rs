use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

pub mod chat {
    tonic::include_proto!("chat");
}

use chat::chat_service_client::ChatServiceClient;
use chat::{ChatMessage, GetMessagesRequest};

use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, MouseButton, MouseEventKind,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, List, ListItem, Paragraph},
};

const N: usize = 10;
const MAX_BUFFER: usize = 3 * N;

struct AppState {
    // Single queue representing the buffer of fetched messages (up to 3N)
    buffer: Arc<Mutex<VecDeque<ChatMessage>>>,
    // Status info
    connected: Arc<Mutex<bool>>,
    last_error: Arc<Mutex<Option<String>>>,
    // Trigger to fetch more messages
    trigger_tx: mpsc::Sender<()>,
    // Store the calculated button rect for mouse click detection
    button_rect: Rect,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("[Client] Connecting to Stream Chat Server at http://[::1]:50051...");

    let buffer = Arc::new(Mutex::new(VecDeque::new()));
    let connected = Arc::new(Mutex::new(false));
    let last_error = Arc::new(Mutex::new(None));
    let (trigger_tx, mut trigger_rx) = mpsc::channel::<()>(10);

    // Setup TUI
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Spawn background task to keep buffer filled
    let buffer_clone = Arc::clone(&buffer);
    let connected_clone = Arc::clone(&connected);
    let error_clone = Arc::clone(&last_error);

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

            // If the buffer has dropped below 66% (2 * N), fetch N messages
            if len <= 2 * N {
                // Attempt to connect and fetch
                match ChatServiceClient::connect("http://[::1]:50051").await {
                    Ok(mut client) => {
                        {
                            *connected_clone.lock().unwrap() = true;
                            *error_clone.lock().unwrap() = None;
                        }

                        // Fetch N messages
                        let request = tonic::Request::new(GetMessagesRequest { limit: N as u32 });

                        match client.get_messages(request).await {
                            Ok(response) => {
                                let new_msgs = response.into_inner().messages;
                                if !new_msgs.is_empty() {
                                    let mut buf = buffer_clone.lock().unwrap();
                                    // Make sure we don't exceed 3N
                                    let space = MAX_BUFFER.saturating_sub(buf.len());
                                    let to_add = std::cmp::min(space, new_msgs.len());
                                    buf.extend(new_msgs.into_iter().take(to_add));
                                }
                            }
                            Err(e) => {
                                *error_clone.lock().unwrap() =
                                    Some(format!("gRPC Call Error: {}", e));
                            }
                        }
                    }
                    Err(e) => {
                        *connected_clone.lock().unwrap() = false;
                        *error_clone.lock().unwrap() = Some(format!("Connection failed: {}", e));
                    }
                }
            }
        }
    });

    let mut state = AppState {
        buffer,
        connected,
        last_error,
        trigger_tx,
        button_rect: Rect::default(),
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
                Event::Key(key) => {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Char('Q') => {
                            return Ok(());
                        }
                        KeyCode::Char(' ') | KeyCode::Char('a') | KeyCode::Char('A') => {
                            // Acknowledge bottom message
                            acknowledge_message(state).await;
                        }
                        _ => {}
                    }
                }
                Event::Mouse(mouse_event) => {
                    if mouse_event.kind == MouseEventKind::Down(MouseButton::Left) {
                        let col = mouse_event.column;
                        let row = mouse_event.row;
                        if state
                            .button_rect
                            .contains(ratatui::layout::Position { x: col, y: row })
                        {
                            // Mouse clicked the acknowledge button!
                            acknowledge_message(state).await;
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

async fn acknowledge_message(state: &mut AppState) {
    let len = {
        let mut buf = state.buffer.lock().unwrap();
        buf.pop_front();
        buf.len()
    };
    if len <= 2 * N {
        let _ = state.trigger_tx.try_send(());
    }
}

fn ui(f: &mut ratatui::Frame, state: &mut AppState) {
    let size = f.size();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(N as u16), Constraint::Length(1)])
        .split(size);

    let buffer_msgs = state.buffer.lock().unwrap().clone();
    let mut list_items = Vec::new();
    let display_count = std::cmp::min(N, buffer_msgs.len());

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

        // Highlight the bottom message (index 0) because it's the next to be acknowledged!
        if i == 0 {
            list_items.push(
                ListItem::new(Line::from(vec![
                    Span::styled(
                        "▶",
                        Style::default()
                            .fg(Color::Yellow)
                            .add_modifier(Modifier::BOLD),
                    ),
                    platform_tag,
                    sender,
                    content,
                ]))
                .style(Style::default().bg(Color::Rgb(40, 40, 20))),
            ); // Subtle highlight background
        } else {
            list_items.push(ListItem::new(Line::from(vec![
                Span::raw(" "),
                platform_tag,
                sender,
                content,
            ])));
        }
    }

    let chat_list = List::new(list_items).block(Block::default());
    f.render_widget(chat_list, chunks[0]);

    // Status Block
    let connected = *state.connected.lock().unwrap();
    let last_err = state.last_error.lock().unwrap().clone();
    let color = match connected {
        true => Color::Green,
        false => Color::Red,
    };
    let conn_status = Span::styled(
        "◉ Connected",
        Style::default().fg(color).add_modifier(Modifier::BOLD),
    );
    let err_msg = match last_err {
        Some(e) => format!(" | Error: {}", e),
        None => "".to_string(),
    };

    let status_text = Line::from(vec![
        conn_status,
        Span::styled(err_msg, Style::default().fg(Color::Red)),
    ]);

    let status_block = Paragraph::new(status_text).block(Block::default());
    f.render_widget(status_block, chunks[1]);
}
