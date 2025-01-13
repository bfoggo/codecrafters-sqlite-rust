use std::{iter::Peekable, slice::Iter};

use crate::typecodes::TypeCode;

use super::{select::SelectStmt, tokenizer::Token, Parse};

#[derive(Debug, Clone)]
pub struct CreateTableStmt {
    temp: bool,
    if_not_exists: bool,
    schema_name: Option<String>,
    pub table_name: String,
    schema_constructor: SchemaConstructor,
}

impl CreateTableStmt {
    pub fn cols(&self) -> Vec<ColumnDef> {
        match &self.schema_constructor {
            SchemaConstructor::FromColumns { columns, .. } => columns.clone(),
            _ => panic!("Cannot Extract Columns from this schema constructor"),
        }
    }
}

impl Parse for CreateTableStmt {
    fn parse(input: &mut Peekable<Iter<Token>>) -> (Self, usize) {
        let mut consumed = 0;
        let temp = match input.peek().unwrap() {
            Token::Temp => {
                input.next();
                consumed += 1;
                true
            }
            _ => false,
        };
        let if_not_exists = match input.peek().unwrap() {
            Token::If => {
                input.next();
                assert_eq!(input.next().unwrap(), &Token::Not);
                assert_eq!(input.next().unwrap(), &Token::Exists);
                consumed += 3;
                true
            }
            _ => false,
        };
        let (schema_name, table_name) = match input.peek().unwrap() {
            Token::Identifier(s) => {
                input.next();
                consumed += 1;
                match input.peek().unwrap() {
                    Token::Operator(t) if *t == ".".to_string() => {
                        input.next();
                        consumed += 1;
                        let table_name = match input.next().unwrap() {
                            Token::Identifier(t) => t.to_string(),
                            _ => panic!("expected table name"),
                        };
                        (Some(s.to_string()), table_name)
                    }
                    _ => (None, s.to_string()),
                }
            }
            _ => panic!("expected schema name or table name"),
        };

        let (schema_constructor, n) = SchemaConstructor::parse(input);
        consumed += n;
        (
            CreateTableStmt {
                temp,
                if_not_exists,
                schema_name,
                table_name,
                schema_constructor,
            },
            consumed,
        )
    }
}

#[derive(Debug, Clone)]
enum SchemaConstructor {
    FromColumns {
        columns: Vec<ColumnDef>,
        table_constraints: Vec<TableConstraint>,
    },
    AsSelect(SelectStmt),
}

impl Parse for SchemaConstructor {
    fn parse(input: &mut Peekable<Iter<Token>>) -> (Self, usize) {
        let mut consumed = 0;
        match input.peek().unwrap() {
            Token::Operator(t) if *t == "(".to_string() => {
                let mut columns = Vec::new();
                let mut table_constraints = Vec::new();
                input.next();
                consumed += 1;
                loop {
                    if input.peek().is_none() {
                        break;
                    }
                    match input.peek().unwrap() {
                        Token::Identifier(_) => {
                            let (column_def, n) = ColumnDef::parse(input);
                            columns.push(column_def);
                            consumed += n;
                        }
                        Token::Operator(t) if *t == ";".to_string() => {
                            input.next();
                            consumed += 1;
                            break;
                        }
                        _ => panic!("unexpected token"),
                    }
                }
                (
                    SchemaConstructor::FromColumns {
                        columns,
                        table_constraints,
                    },
                    consumed,
                )
            }
            Token::As => {
                input.next();
                consumed += 1;
                let (select_stmt, n) = SelectStmt::parse(input);
                (SchemaConstructor::AsSelect(select_stmt), consumed + n)
            }
            _ => panic!("unexpected token"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    type_: Option<TypeCode>,
    constraints: Vec<ColumnConstraint>,
}

impl Parse for ColumnDef {
    fn parse(input: &mut Peekable<Iter<Token>>) -> (Self, usize) {
        let mut consumed = 0;
        let name = match input.next().unwrap() {
            Token::Identifier(name) => {
                consumed += 1;
                name.to_string()
            }
            _ => panic!("expected column name"),
        };
        let type_ = match input.peek().unwrap() {
            Token::Identifier(s) => {
                let type_code = TypeCode::parse_str(s);
                input.next();
                consumed += 1;
                Some(type_code)
            }
            _ => None,
        };
        let mut constraints = Vec::new();
        loop {
            match input.peek().unwrap() {
                Token::Operator(t) if *t == ",".to_string() => {
                    input.next();
                    consumed += 1;
                    break;
                }
                Token::Operator(t) if *t == ")".to_string() => {
                    input.next();
                    consumed += 1;
                    break;
                }
                _ => {
                    let new_constraint = ColumnConstraint::parse(input);
                    consumed += new_constraint.1;
                    constraints.push(new_constraint.0);
                }
            }
        }
        (
            ColumnDef {
                name,
                type_,
                constraints,
            },
            consumed,
        )
    }
}

#[derive(Debug, Clone)]
enum ColumnConstraint {
    Name(Option<String>),
    PrimaryKey {
        ord_: Option<SortOrder>,
        conflict_clause: Option<ConflictClause>,
        autoincrement: bool,
    },
    NotNull {
        conflict_clause: Option<ConflictClause>,
    },
    Unique,
    Check,
    Default,
    Collate,
    ForeignKey(ForeignKeyClause),
}

impl Parse for ColumnConstraint {
    fn parse(input: &mut Peekable<Iter<Token>>) -> (Self, usize) {
        let mut consumed = 0;
        let constraint = match input.peek().unwrap() {
            Token::Primary => {
                input.next();
                consumed += 1;
                assert_eq!(input.next().unwrap(), &Token::Key);
                consumed += 1;
                let ord_ = match input.peek().unwrap() {
                    Token::Asc => {
                        input.next();
                        consumed += 1;
                        Some(SortOrder::Asc)
                    }
                    Token::Desc => {
                        input.next();
                        consumed += 1;
                        Some(SortOrder::Desc)
                    }
                    _ => None,
                };
                let conflict_clause = match input.peek().unwrap() {
                    Token::On => {
                        input.next();
                        consumed += 1;
                        assert_eq!(input.next().unwrap(), &Token::Conflict);
                        consumed += 2;
                        let (clause, n) = ConflictClause::parse(input);
                        consumed += n;
                        Some(clause)
                    }
                    _ => None,
                };
                let autoincrement = match input.peek().unwrap() {
                    Token::Autoincrement => {
                        input.next();
                        consumed += 1;
                        true
                    }
                    _ => false,
                };
                ColumnConstraint::PrimaryKey {
                    ord_,
                    conflict_clause,
                    autoincrement,
                }
            }
            Token::Not => {
                input.next();
                consumed += 1;
                assert_eq!(input.next().unwrap(), &Token::Null);
                consumed += 1;
                let conflict_clause = match input.peek().unwrap() {
                    Token::On => {
                        input.next();
                        consumed += 1;
                        assert_eq!(input.next().unwrap(), &Token::Conflict);
                        consumed += 2;
                        let (clause, n) = ConflictClause::parse(input);
                        consumed += n;
                        Some(clause)
                    }
                    _ => None,
                };
                ColumnConstraint::NotNull { conflict_clause }
            }
            Token::Unique => {
                input.next();
                consumed += 1;
                ColumnConstraint::Unique
            }
            Token::Check => {
                input.next();
                consumed += 1;
                ColumnConstraint::Check
            }
            Token::Default => {
                input.next();
                consumed += 1;
                ColumnConstraint::Default
            }
            Token::Collate => {
                input.next();
                consumed += 1;
                ColumnConstraint::Collate
            }
            Token::Foreign => {
                input.next();
                consumed += 1;
                assert_eq!(input.next().unwrap(), &Token::Key);
                consumed += 1;
                let (foreign_key, n) = ForeignKeyClause::parse(input);
                consumed += n;
                ColumnConstraint::ForeignKey(foreign_key)
            }
            _ => panic!("unexpected token"),
        };
        (constraint, consumed)
    }
}

#[derive(Debug, Clone)]
enum SortOrder {
    Asc,
    Desc,
}

impl Parse for SortOrder {
    fn parse(input: &mut Peekable<Iter<Token>>) -> (Self, usize) {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
enum ConflictClause {
    Rollback,
    Abort,
    Fail,
    Ignore,
    Replace,
}

impl Parse for ConflictClause {
    fn parse(input: &mut Peekable<Iter<Token>>) -> (Self, usize) {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
struct ForeignKeyClause {
    foreign_table: ForeignKeyTable,
}

impl Parse for ForeignKeyClause {
    fn parse(mut input: &mut Peekable<Iter<Token>>) -> (Self, usize) {
        let mut consumed = 0;
        assert_eq!(input.next().unwrap(), &Token::Operator("(".to_string()));
        consumed += 1;
        let mut column_names = Vec::new();
        loop {
            match input.next().unwrap() {
                Token::Identifier(column_name) => {
                    column_names.push(column_name.to_string());
                    consumed += 1;
                }
                Token::Operator(t) if *t == ",".to_string() => {
                    consumed += 1;
                }
                Token::Operator(t) if *t == "(".to_string() => {
                    consumed += 1;
                    break;
                }
                _ => panic!("unexpected token"),
            }
        }
        let foreign_table = ForeignKeyTable {
            schema_name: None,
            table_name: match input.next().unwrap() {
                Token::Identifier(table_name) => table_name.to_string(),
                _ => panic!("expected table name"),
            },
            column_names,
        };
        (ForeignKeyClause { foreign_table }, consumed)
    }
}

#[derive(Debug, Clone)]
struct ForeignKeyTable {
    schema_name: Option<String>,
    table_name: String,
    column_names: Vec<String>,
}

impl Parse for ForeignKeyTable {
    fn parse(input: &mut Peekable<Iter<Token>>) -> (Self, usize) {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
struct TableConstraint;

impl Parse for TableConstraint {
    fn parse(input: &mut Peekable<Iter<Token>>) -> (Self, usize) {
        unimplemented!()
    }
}
