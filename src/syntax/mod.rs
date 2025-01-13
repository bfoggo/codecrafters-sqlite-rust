use std::{iter::Peekable, slice::Iter};
use tokenizer::Token;

pub mod create_table;
pub mod select;
pub mod statement;
pub mod tokenizer;

pub trait Parse {
    fn parse(input: &mut Peekable<Iter<Token>>) -> (Self, usize)
    where
        Self: Sized;
}

pub fn parse(input: &str) -> statement::Statement {
    let tokens = tokenizer::tokenize(input);
    let mut iter = tokens.iter().peekable();
    let stmt = statement::Statement::parse(&mut iter);
    stmt.0
}
