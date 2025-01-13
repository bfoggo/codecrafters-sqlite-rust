use std::{iter::Peekable, slice::Iter};

use super::{tokenizer::Token, Parse};

#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub columns: Vec<String>,
    pub table: String,
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
        (SelectStmt { columns, table }, consumed)
    }
}
