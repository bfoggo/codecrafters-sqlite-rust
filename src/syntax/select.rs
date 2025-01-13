use std::{iter::Peekable, slice::Iter};

use super::{tokenizer::Token, Parse};

#[derive(Debug, Clone)]
pub struct SelectStmt;

impl Parse for SelectStmt {
    fn parse(_input: &mut Peekable<Iter<Token>>) -> (Self, usize) {
        unimplemented!()
    }
}
