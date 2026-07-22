use super::validate_task_title;

#[test]
fn task_title_accepts_up_to_five_words_and_trims_whitespace() {
    assert_eq!(
        validate_task_title("  Исправь порядок сообщений в UI  ").expect("valid title"),
        "Исправь порядок сообщений в UI"
    );
}

#[test]
fn task_title_rejects_empty_values() {
    assert!(validate_task_title(" \n\t ").is_err());
}

#[test]
fn task_title_rejects_more_than_five_words() {
    assert!(validate_task_title("one two three four five six").is_err());
}
