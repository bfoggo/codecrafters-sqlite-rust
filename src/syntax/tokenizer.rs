enum CharacterClass {
    Whitespace,
    Alphabetic,
    Numeric,
    Special,
}

fn classify(c: &char) -> CharacterClass {
    match c {
        '\u{0009}'
        | '\u{000A}'
        | '\u{000B}'
        | '\u{000C}'
        | '\u{000D}'
        | ' '
        | '\u{0085}'
        | '\u{00A0}'
        | '\u{1680}'
        | '\u{2000}'..='\u{200A}'
        | '\u{2028}'
        | '\u{2029}'
        | '\u{202F}'
        | '\u{205F}'
        | '\u{3000}' => CharacterClass::Whitespace,
        '\u{0041}'..='\u{005A}' | '\u{0061}'..='\u{007A}' | '\u{000f}' | '\u{007f}'.. => {
            CharacterClass::Alphabetic
        }
        '\u{0030}'..='\u{0039}' => CharacterClass::Numeric,
        _ => CharacterClass::Special,
    }
}

fn is_alphanumeric(c: &char) -> bool {
    match classify(c) {
        CharacterClass::Alphabetic | CharacterClass::Numeric => true,
        _ => false,
    }
}

#[derive(Debug, PartialEq)]
pub enum Token {
    Whitespace,
    Identifier(String),
    Literal(LiteralKind),
    Variable(String),
    Operator(String),

    // keywords
    Abort,
    Add,
    After,
    All,
    Alter,
    Analyze,
    And,
    As,
    Asc,
    Attach,
    Autoincrement,
    Before,
    Begin,
    Between,
    By,
    Cascade,
    Case,
    Cast,
    Check,
    Collate,
    Column,
    Commit,
    Conflict,
    Constraint,
    Create,
    Cross,
    CurrentDate,
    CurrentTime,
    CurrentTimestamp,
    Database,
    Default,
    Deferrable,
    Deferred,
    Delete,
    Desc,
    Detach,
    Distinct,
    Drop,
    Each,
    Else,
    Escape,
    Except,
    Exclusive,
    Exists,
    Explain,
    Fail,
    For,
    Foreign,
    From,
    Full,
    Glob,
    Group,
    Having,
    If,
    Ignore,
    Immediate,
    In,
    Index,
    Indexed,
    Initially,
    Inner,
    Insert,
    Instead,
    Intersect,
    Into,
    Is,
    IsNull,
    Join,
    Key,
    Left,
    Like,
    Limit,
    Match,
    Natural,
    No,
    Not,
    NotNull,
    Null,
    Of,
    Offset,
    On,
    Or,
    Order,
    Outer,
    Plan,
    Pragma,
    Primary,
    Query,
    Raise,
    Recursive,
    References,
    Regexp,
    Reindex,
    Release,
    Rename,
    Replace,
    Restrict,
    Right,
    Rollback,
    Row,
    Rows,
    Savepoint,
    Select,
    Set,
    Table,
    Temp,
    Temporary,
    Then,
    To,
    Transaction,
    Trigger,
    Union,
    Unique,
    Update,
    Using,
    Vacuum,
    Values,
    View,
    Virtual,
    When,
    Where,
    Window,
    With,
    Without,
}

#[derive(Debug, PartialEq)]
pub enum LiteralKind {
    Str(String),
    Blob(String),
    Integer(String),
    Float(String),
}

pub fn tokenize(input: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut iter = input.chars().peekable();
    while let Some(c) = iter.next() {
        match classify(&c) {
            CharacterClass::Whitespace => {
                continue;
            }
            CharacterClass::Alphabetic => {
                let mut ident = String::new();
                ident.push(c);
                while let Some(&c) = iter.peek() {
                    if is_alphanumeric(&c) {
                        ident.push(c);
                        iter.next();
                    } else {
                        break;
                    }
                }
                tokens.push(match ident.to_lowercase().as_str() {
                    "abort" => Token::Abort,
                    "add" => Token::Add,
                    "after" => Token::After,
                    "all" => Token::All,
                    "alter" => Token::Alter,
                    "analyze" => Token::Analyze,
                    "and" => Token::And,
                    "as" => Token::As,
                    "asc" => Token::Asc,
                    "attach" => Token::Attach,
                    "autoincrement" => Token::Autoincrement,
                    "before" => Token::Before,
                    "begin" => Token::Begin,
                    "between" => Token::Between,
                    "by" => Token::By,
                    "cascade" => Token::Cascade,
                    "case" => Token::Case,
                    "cast" => Token::Cast,
                    "check" => Token::Check,
                    "collate" => Token::Collate,
                    "column" => Token::Column,
                    "commit" => Token::Commit,
                    "conflict" => Token::Conflict,
                    "constraint" => Token::Constraint,
                    "create" => Token::Create,
                    "cross" => Token::Cross,
                    "current_date" => Token::CurrentDate,
                    "current_time" => Token::CurrentTime,
                    "current_timestamp" => Token::CurrentTimestamp,
                    "database" => Token::Database,
                    "default" => Token::Default,
                    "deferrable" => Token::Deferrable,
                    "deferred" => Token::Deferred,
                    "delete" => Token::Delete,
                    "desc" => Token::Desc,
                    "detach" => Token::Detach,
                    "distinct" => Token::Distinct,
                    "drop" => Token::Drop,
                    "each" => Token::Each,
                    "else" => Token::Else,
                    "escape" => Token::Escape,
                    "except" => Token::Except,
                    "exclusive" => Token::Exclusive,
                    "exists" => Token::Exists,
                    "explain" => Token::Explain,
                    "fail" => Token::Fail,
                    "for" => Token::For,
                    "foreign" => Token::Foreign,
                    "from" => Token::From,
                    "full" => Token::Full,
                    "glob" => Token::Glob,
                    "group" => Token::Group,
                    "having" => Token::Having,
                    "if" => Token::If,
                    "ignore" => Token::Ignore,
                    "immediate" => Token::Immediate,
                    "in" => Token::In,
                    "index" => Token::Index,
                    "indexed" => Token::Indexed,
                    "initially" => Token::Initially,
                    "inner" => Token::Inner,
                    "insert" => Token::Insert,
                    "instead" => Token::Instead,
                    "intersect" => Token::Intersect,
                    "into" => Token::Into,
                    "is" => Token::Is,
                    "isnull" => Token::IsNull,
                    "join" => Token::Join,
                    "key" => Token::Key,
                    "left" => Token::Left,
                    "like" => Token::Like,
                    "limit" => Token::Limit,
                    "match" => Token::Match,
                    "natural" => Token::Natural,
                    "no" => Token::No,
                    "not" => Token::Not,
                    "notnull" => Token::NotNull,
                    "null" => Token::Null,
                    "of" => Token::Of,
                    "offset" => Token::Offset,
                    "on" => Token::On,
                    "or" => Token::Or,
                    "order" => Token::Order,
                    "outer" => Token::Outer,
                    "plan" => Token::Plan,
                    "pragma" => Token::Pragma,
                    "primary" => Token::Primary,
                    "query" => Token::Query,
                    "raise" => Token::Raise,
                    "recursive" => Token::Recursive,
                    "references" => Token::References,
                    "regexp" => Token::Regexp,
                    "reindex" => Token::Reindex,
                    "release" => Token::Release,
                    "rename" => Token::Rename,
                    "replace" => Token::Replace,
                    "restrict" => Token::Restrict,
                    "right" => Token::Right,
                    "rollback" => Token::Rollback,
                    "row" => Token::Row,
                    "rows" => Token::Rows,
                    "savepoint" => Token::Savepoint,
                    "select" => Token::Select,
                    "set" => Token::Set,
                    "table" => Token::Table,
                    "temp" => Token::Temp,
                    "temporary" => Token::Temporary,
                    "then" => Token::Then,
                    "to" => Token::To,
                    "transaction" => Token::Transaction,
                    "trigger" => Token::Trigger,
                    "union" => Token::Union,
                    "unique" => Token::Unique,
                    "update" => Token::Update,
                    "using" => Token::Using,
                    "vacuum" => Token::Vacuum,
                    "values" => Token::Values,
                    "view" => Token::View,
                    "virtual" => Token::Virtual,
                    "when" => Token::When,
                    "where" => Token::Where,
                    "window" => Token::Window,
                    "with" => Token::With,
                    "without" => Token::Without,
                    _ => Token::Identifier(ident),
                });
            }
            CharacterClass::Numeric => {
                let mut number = String::new();
                number.push(c);
                let mut is_float = false;
                while let Some(&c) = iter.peek() {
                    if c.is_digit(10) {
                        number.push(c);
                        iter.next();
                    } else if c == '.' {
                        if is_float {
                            break;
                        }
                        is_float = true;
                        number.push(c);
                        iter.next();
                    } else {
                        break;
                    }
                }
                if is_float {
                    tokens.push(Token::Literal(LiteralKind::Float(number)));
                } else {
                    tokens.push(Token::Literal(LiteralKind::Integer(number)));
                }
            }
            CharacterClass::Special => {
                if c == '-' {
                    if let Some(&next_c) = iter.peek() {
                        if next_c == '-' {
                            iter.next();
                            while let Some(&c) = iter.peek() {
                                if c == '\n' {
                                    break;
                                }
                                iter.next();
                            }
                            continue;
                        }
                    }
                } else if c == '/' {
                    if let Some(&next_c) = iter.peek() {
                        if next_c == '*' {
                            iter.next();
                            while let Some(c) = iter.next() {
                                if c == '*' {
                                    if let Some(&next_c) = iter.peek() {
                                        if next_c == '/' {
                                            iter.next();
                                            break;
                                        }
                                    }
                                }
                            }
                            continue;
                        }
                    }
                }
                tokens.push(Token::Operator(c.to_string()));
            }
        }
    }
    tokens
}
