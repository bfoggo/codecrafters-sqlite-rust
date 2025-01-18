use std::iter::Peekable;
use std::slice::Iter;

use crate::syntax::create_table::CreateTableStmt;
use crate::typecodes::TypeCode;

use super::select::SelectStmt;
use super::tokenizer::Token;
use super::Parse;
use super::create_index::CreateIndexStmt;

#[derive(Debug)]
pub enum Statement {
    AlterTable,
    Analyze,
    Attach,
    Begin,
    Commit,
    CreateIndex{stmt: CreateIndexStmt, unique: bool},
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
    Select(SelectStmt),
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
                        let (stmt, n) = CreateTableStmt::parse(input);
                        (Statement::CreateTable(stmt), n)
                    },
                    Token::Unique => {
                        input.next();
                        let (stmt, n) = CreateIndexStmt::parse(input);
                        (Statement::CreateIndex{stmt, unique: false}, n)
                    },
                    Token::Index => {
                        let (stmt, n) = CreateIndexStmt::parse(input);
                        (Statement::CreateIndex{stmt, unique: false}, n)
                    },
                    _ => unimplemented!(),
                }
            }
            Token::Select => {
                let (stmt, consumed) = SelectStmt::parse(input);
                (Statement::Select(stmt), consumed)
            }
            _ => unimplemented!(),
        }
    }
}
