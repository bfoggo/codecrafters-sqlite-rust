use std::iter::Peekable;
use std::slice::Iter;

use crate::syntax::create_table::CreateTableStmt;
use crate::typecodes::TypeCode;

use super::tokenizer::Token;
use super::Parse;

#[derive(Debug)]
pub enum Statement {
    AlterTable,
    Analyze,
    Attach,
    Begin,
    Commit,
    CreateIndex,
    CreateTable(CreateTableStmt),
    CreateTrigger,
    CreateView,
    CreateVirtualTable,
    Delete,
    DeleteFrom,
    Detach,
    DropIndex,
    DropTable,
    DropTrigger,
    DropView,
    Insert,
    Pragma,
    Reindex,
    Release,
    Rollback,
    Savepoint,
    Select,
    Update,
    Vacuum,
}

impl Parse for Statement {
    fn parse(input: &mut Peekable<Iter<Token>>) -> (Self, usize) {
        let token = input.next().unwrap();
        match token {
            Token::Create => {
                let token = input.next().unwrap();
                match token {
                    Token::Table => {
                        let (stmt, _) = CreateTableStmt::parse(input);
                        (Statement::CreateTable(stmt), 0)
                    }
                    _ => unimplemented!(),
                }
            }
            _ => unimplemented!(),
        }
    }
}
