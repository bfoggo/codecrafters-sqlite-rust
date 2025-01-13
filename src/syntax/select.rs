use std::{iter::Peekable, slice::Iter};

use super::{
    tokenizer::{LiteralKind, Token},
    Parse,
};

#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub columns: Vec<String>,
    pub table: String,
    pub where_clause: Vec<WhereClause>,
}

impl Parse for SelectStmt {
    fn parse(input: &mut Peekable<Iter<Token>>) -> (Self, usize) {
        let mut consumed = 0;
        let mut columns = Vec::new();
        loop {
            match input.peek().unwrap() {
                Token::Identifier(ident) => {
                    columns.push(ident.clone());
                    consumed += 1;
                    input.next();
                }
                Token::From => {
                    consumed += 1;
                    input.next();
                    break;
                }
                Token::Operator(t) if t == "," => {
                    consumed += 1;
                    input.next();
                }
                _ => {
                    panic!("Expected identifier or FROM");
                }
            }
        }
        let table = match input.next().unwrap() {
            Token::Identifier(ident) => ident.clone(),
            _ => panic!("Expected identifier after FROM"),
        };
        consumed += 1;

        let mut where_clause = Vec::new();
        match input.peek() {
            Some(Token::Where) => {
                consumed += 1;
                input.next();
                loop {
                    let (clause, clause_consumed) = WhereClause::parse(input);
                    where_clause.push(clause);
                    consumed += clause_consumed;
                    match input.peek() {
                        Some(Token::And) => {
                            consumed += 1;
                            input.next();
                        }
                        _ => {
                            break;
                        }
                    }
                }
            }
            _ => {}
        }
        (
            SelectStmt {
                columns,
                table,
                where_clause,
            },
            consumed,
        )
    }
}

#[derive(Debug, Clone)]
pub struct WhereClause {
    pub column: String,
    pub value: String,
}

impl Parse for WhereClause {
    fn parse(input: &mut Peekable<Iter<Token>>) -> (Self, usize) {
        let mut consumed = 0;
        let column = match input.next().unwrap() {
            Token::Identifier(ident) => ident.clone(),
            _ => panic!("Expected identifier"),
        };
        let _ = match input.next().unwrap() {
            Token::Operator(t) if t == "=" => {
                consumed += 1;
            }
            _ => panic!("Expected ="),
        };
        consumed += 1;
        let value = match input.next().unwrap() {
            Token::Literal(lit) => match lit {
                LiteralKind::Str(s)
                | LiteralKind::Blob(s)
                | LiteralKind::Integer(s)
                | LiteralKind::Real(s) => s.clone(),
                _ => panic!("Expected string literal"),
            },
            _ => panic!("Expected literal"),
        };
        consumed += 1;
        (WhereClause { column, value }, consumed)
    }
}
