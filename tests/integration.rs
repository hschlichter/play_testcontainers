use play_testcontainers::hello;

#[test]
fn test_hello() {
    let msg = hello();
    assert_eq!(msg, "Hello World");
}
