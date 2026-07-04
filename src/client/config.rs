pub struct ClientConfig {
    pub limit: usize,
}

impl ClientConfig {
    pub fn parse_args() -> Self {
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
        Self { limit: n }
    }
}
