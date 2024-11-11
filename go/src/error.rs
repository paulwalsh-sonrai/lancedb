


pub fn convert_error(e: impl std::fmt::Debug) -> String {
    format!("Error: {:?}", e)
}


// Helper function for indentation
fn indent_string(s: &str, indent: usize) -> String {
    let indent_spaces = " ".repeat(indent);
    format!("{}{}", indent_spaces, s)
}
