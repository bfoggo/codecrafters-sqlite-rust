use std::{iter::Peekable, mem::uninitialized, slice::Iter};

use crate::page_io::Column;

use super::{create_table::ColumnDef, select::WhereClause, tokenizer::Token, Parse};

#[derive(Debug)]
pub struct CreateIndexStmt {
    pub if_not_exists: bool,
    pub schema_name: Option<String>,
    pub index_name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub where_clause: Option<WhereClause>,
}

impl Parse for CreateIndexStmt {
    fn parse(input: &mut Peekable<Iter<Token>>) -> (Self, usize) {
        let mut stmt_mut = CreateIndexStmt {
            if_not_exists: false,
            schema_name: None,
            index_name: "".to_string(),
            table_name: "".to_string(),
            columns: Vec::new(),
            where_clause: None,
        };
        let mut consumed_tokens = 0;
        let token = input.next().unwrap();
        consumed_tokens += 1;
        match token {
            Token::If => {
                assert_eq!(
                    *input.next().unwrap(),
                    Token::Not,
                    "expected NOT after If in Create Index Statment"
                );
                consumed_tokens += 1;
                assert_eq!(
                    *input.next().unwrap(),
                    Token::Exists,
                    "expected EXISTS after IF NOT in Create Index Statement"
                );
                consumed_tokens += 1;
                stmt_mut.if_not_exists = true;
            }
            Token::Identifier(iden) => {
                let split_at_period = iden.split(".").collect::<Vec<&str>>();
                if split_at_period.len() == 1 {
                    stmt_mut.index_name = split_at_period.first().unwrap().to_string();
                } else if split_at_period.len() == 2 {
                    stmt_mut.schema_name = Some(split_at_period.first().unwrap().to_string());
                    stmt_mut.index_name = split_at_period.iter().nth(2).unwrap().to_string();
                } else {
                    panic!("Too many periods in a schema/table name at Create Index")
                }
            }
            _ => {
                panic!("Cannot parse Create Index Statement - unknown token immediately after Index keyword");
            }
        }
        consumed_tokens += 1;
        assert_eq!(
            *input.next().unwrap(),
            Token::On,
            "Expected ON after index name in Create Index Statement"
        );
        consumed_tokens += 1;
        match input.next().unwrap() {
            Token::Identifier(iden) => {
                stmt_mut.table_name = iden.clone();
            }
            _ => {
                panic!("Create Index Statement contains no name");
            }
        }
        consumed_tokens += 1;
        match input.next().unwrap() {
            Token::Operator(op) if op == "(" => {
                consumed_tokens += 1;
                while let Token::Identifier(column_name) = input.next().unwrap() {
                    stmt_mut.columns.push(column_name.to_string());
                    consumed_tokens += 1;
                    match input.peek() {
                        Some(Token::Operator(op)) if op == "," => {
                            input.next();
                            consumed_tokens += 1;
                        }
                        Some(Token::Operator(op)) if op == ")" => break,
                        _ => panic!("Expected ',' or ')' in column list of Create Index Statement"),
                    }
                }
                assert_eq!(
                    *input.next().unwrap(),
                    Token::Operator(")".to_string()),
                    "Expected ')' at the end of column list in Create Index Statement"
                );
                consumed_tokens += 1;
            }
            _ => panic!("Expected '(' after table name in Create Index Statement"),
        }

        if let Some(Token::Where) = input.peek() {
            input.next();
            consumed_tokens += 1;
            let (where_clause, where_consumed) = WhereClause::parse(input);
            stmt_mut.where_clause = Some(where_clause);
            consumed_tokens += where_consumed;
        }

        (stmt_mut, consumed_tokens)
    }
}
