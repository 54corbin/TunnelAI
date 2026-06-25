use tunnel_ai::Error;

#[test]
fn error_display_includes_kind() {
    let err = Error::HttpParse("bad version".into());
    assert!(err.to_string().contains("bad version"));
}

#[test]
fn error_kinds_are_distinct() {
    assert_ne!(
        std::mem::discriminant(&Error::HttpParse("x".into())),
        std::mem::discriminant(&Error::RequestValidation("x".into())),
    );
}
