pub mod file_order;
use crate::query::file_order::{FileOrder, OrderedFileSummary};
use crate::services::query::Retriever;
use crate::tag::TagId;
use im::HashSet;
use rand_chacha::ChaCha12Rng;
use scalable_cuckoo_filter::{DefaultHasher, ScalableCuckooFilter};
use serde::{Deserialize, Serialize};
use std::fmt;

peg::parser! {
    grammar tag_query() for str {
        pub rule expression() -> Result<Formula, QueryErr>
            = precedence! {
                x:(@) _ "OR" _ y:@ { Ok(Formula::BinaryExpression((BinaryOp::OR), (Box::new(x?)), (Box::new(y?)))) }
                --
                x:(@) _ "XOR" _ y:@ { Ok(Formula::BinaryExpression((BinaryOp::XOR), (Box::new(x?)), (Box::new(y?)))) }
                --
                x:(@) _ "AND" _ y:@ { Ok(Formula::BinaryExpression((BinaryOp::AND), (Box::new(x?)), (Box::new(y?)))) }
                --
                "NOT" _ x:@ { Ok(Formula::UnaryExpression((UnaryOp::NOT), (Box::new(x?)))) }
                --
                t:term() {
                    Ok(Formula::Proposition(Proposition { tag_id: t.parse::<TagId>().map_err(|_| QueryErr::ParseError)? }))
                }
                --
                "(" _ e:expression() _ ")" { e }
            }

        rule term() -> &'input str
            = "\"" t:$([^ '"']+) "\"" { t }

        rule _() = quiet!{[' ' | '\t' | '\n']*} // Ignore spaces, tabs, and newlines
    }
}

#[derive(Clone, Eq, Serialize, Deserialize, PartialEq)]
enum Formula {
    Proposition(Proposition),
    Constant(bool),
    BinaryExpression(BinaryOp, Box<Formula>, Box<Formula>),
    UnaryExpression(UnaryOp, Box<Formula>),
}

impl Formula {
    fn get_tags(&self) -> HashSet<TagId> {
        match self {
            Formula::BinaryExpression(_, x, y) => x
                .get_tags()
                .union(y.get_tags())
                .into_iter()
                .collect::<HashSet<TagId>>(),
            Formula::UnaryExpression(_, x) => x.get_tags(),
            Formula::Proposition(p) => HashSet::from(Vec::from(&[p.tag_id])),
            Formula::Constant(_) => HashSet::new(), //[!] All tags if True
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
enum BinaryOp {
    AND,
    OR,
    XOR,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
enum UnaryOp {
    NOT,
}

#[derive(Eq, PartialEq, Hash, Clone, Serialize, Deserialize)]
struct Proposition {
    tag_id: TagId,
}

impl fmt::Debug for Proposition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.tag_id)
    }
}

impl fmt::Debug for Formula {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Formula::Proposition(p) => {
                write!(f, "{:?}", p)
            }
            Formula::Constant(c) => {
                if *c {
                    write!(f, "TRUE")
                } else {
                    write!(f, "FALSE")
                }
            }
            Formula::BinaryExpression(op, a, b) => {
                write!(f, "({:?} {:?} {:?})", a, op, b)
            }
            Formula::UnaryExpression(op, a) => {
                write!(f, "({:?} {:?})", op, a)
            }
        }
    }
}
//[/] Own implementation of decode can enforce that BTreeSet contains files of the specified FileOrder
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Query {
    formula: Formula,
    pub order: FileOrder,
    pub ascending: bool,
}

impl Query {
    pub fn new(query: &str, order: FileOrder, ascending: bool) -> Result<Self, QueryErr> {
        let formula = tag_query::expression(query).map_err(|_err| QueryErr::SyntaxError)??;

        let mut query = Query {
            formula,
            ascending,
            order,
        };
        query.simplify();
        Ok(query)
    }

    pub fn get_tags(&self) -> HashSet<TagId> {
        self.formula.get_tags()
    }
    pub fn may_hold(
        &mut self,
        tags: &ScalableCuckooFilter<TagId, DefaultHasher, ChaCha12Rng>, // Tags that may be present in a shelf (can contain false positives)
    ) -> bool {
        self.formula.may_hold(tags)
    }

    //[!] Tags should be Validated inside the QueryService

    pub fn evaluate(
        //[/] Only local shelves
        &mut self,
        retriever: Retriever,
    ) -> Result<HashSet<OrderedFileSummary>, QueryErr> {
        Query::recursive_evaluate(&self.formula, &retriever)
    }

    fn recursive_evaluate(
        formula: &Formula,
        ret_srv: &Retriever,
    ) -> Result<HashSet<OrderedFileSummary>, QueryErr> {
        //[!] Execute concurrently where possible
        match formula {
            Formula::BinaryExpression(BinaryOp::AND, x, y) => match (x.as_ref(), y.as_ref()) {
                (_, Formula::UnaryExpression(UnaryOp::NOT, b)) => {
                    let a = Query::recursive_evaluate(x, ret_srv)?;
                    let b = Query::recursive_evaluate(b, ret_srv)?;
                    Ok(a.difference(b))
                }
                (Formula::UnaryExpression(UnaryOp::NOT, a), _) => {
                    let a = Query::recursive_evaluate(a, ret_srv)?;
                    let b = Query::recursive_evaluate(y, ret_srv)?;
                    Ok(b.difference(a))
                }
                _ => {
                    let a = Query::recursive_evaluate(x, ret_srv)?;
                    let b = Query::recursive_evaluate(y, ret_srv)?;
                    Ok(a.intersection(b))
                }
            },
            Formula::BinaryExpression(BinaryOp::OR, x, y) => {
                let mut a = Query::recursive_evaluate(x, ret_srv)?;
                let b = Query::recursive_evaluate(y, ret_srv)?;
                a.extend(b); // equivalent to union, slightly more efficient
                Ok(a)
            }
            Formula::BinaryExpression(BinaryOp::XOR, x, y) => {
                let a = Query::recursive_evaluate(x, ret_srv)?;
                let b = Query::recursive_evaluate(y, ret_srv)?;
                Ok(a.symmetric_difference(b))
            }
            Formula::UnaryExpression(UnaryOp::NOT, x) => {
                let all = ret_srv.get_all().map_err(QueryErr::RuntimeError)?;
                let subset = Query::recursive_evaluate(x, ret_srv)?;
                Ok(all.difference(subset))
            }
            Formula::Constant(false) => Ok(HashSet::new()),
            Formula::Constant(true) => ret_srv.get_all().map_err(QueryErr::RuntimeError),
            Formula::Proposition(p) => ret_srv.get(p.tag_id).map_err(QueryErr::RuntimeError),
        }
    }

    fn simplify(&mut self) {
        loop {
            let (formula, changed) = Formula::recursive_simplify(self.formula.clone());
            self.formula = formula;
            if !changed {
                break;
            }
        }
    }
}

impl Formula {
    fn recursive_simplify(formula: Formula) -> (Formula, bool) {
        match formula {
            Formula::Proposition(_) => (formula, false),
            Formula::Constant(_) => (formula, false),
            Formula::UnaryExpression(UnaryOp::NOT, x) => match *x {
                // ¬T ⊨ ⊥, ¬⊥ ⊨ T
                Formula::Constant(c) => (Formula::Constant(!c), true),
                // Double Negation: ¬¬A ⊨ A
                Formula::UnaryExpression(UnaryOp::NOT, y) => {
                    (Formula::recursive_simplify(*y).0, true)
                }
                // No Immediate Simplification - Recursive Step
                _ => {
                    let simplified_x = Formula::recursive_simplify(*x);
                    (
                        Formula::UnaryExpression(UnaryOp::NOT, Box::new(simplified_x.0)),
                        simplified_x.1,
                    )
                }
            },
            Formula::BinaryExpression(BinaryOp::AND, x, y) => {
                match (x.as_ref(), y.as_ref()) {
                    // Annihilation Law: ⊥ ∧ ? ⊨ ⊥
                    (Formula::Constant(false), _) => (Formula::Constant(false), true),
                    // Annihilation Law: ? ∧ ⊥ ⊨ ⊥
                    (_, Formula::Constant(false)) => (Formula::Constant(false), true),
                    // Identity Law: T ∧ A ⊨ A
                    (Formula::Constant(true), _) => (Formula::recursive_simplify(*y).0, true),
                    // Identity Law: A ∧ T ⊨ A
                    (_, Formula::Constant(true)) => (Formula::recursive_simplify(*x).0, true),
                    // Contradiction: ¬A ∧ A ⊨ ⊥
                    (Formula::UnaryExpression(UnaryOp::NOT, a), _) if *a == y => {
                        (Formula::Constant(false), true)
                    }
                    // Contradiction: A ∧ ¬A ⊨ ⊥
                    (_, Formula::UnaryExpression(UnaryOp::NOT, b)) if x == *b => {
                        (Formula::Constant(false), true)
                    }
                    // Absorption Law: A ∧ (A ∨ ?) ⊨ A
                    (_, Formula::BinaryExpression(BinaryOp::OR, a, _)) if x == *a => {
                        (Formula::recursive_simplify(*x).0, true)
                    }
                    // Absorption Law: A ∧ (? ∨ A) ⊨ A
                    (_, Formula::BinaryExpression(BinaryOp::OR, _, b)) if x == *b => {
                        (Formula::recursive_simplify(*x).0, true)
                    }
                    // Absorption Law: (A ∨ ?) ∧ A ⊨ A
                    (Formula::BinaryExpression(BinaryOp::OR, a, _), _) if *a == y => {
                        (Formula::recursive_simplify(*y).0, true)
                    }
                    // Absorption Law: (? ∨ A) ∧ A ⊨ A
                    (Formula::BinaryExpression(BinaryOp::OR, _, b), _) if *b == y => {
                        (Formula::recursive_simplify(*y).0, true)
                    }
                    // Idempotency Law: A ∧ A ⊨ A
                    _ if x == y => (Formula::recursive_simplify(*x).0, true),
                    // De Morgan's Law: ¬A ∧ ¬B ⊨ ¬(A ∨ B)
                    (
                        Formula::UnaryExpression(UnaryOp::NOT, a),
                        Formula::UnaryExpression(UnaryOp::NOT, b),
                    ) => (
                        Formula::UnaryExpression(
                            UnaryOp::NOT,
                            Box::new(Formula::BinaryExpression(
                                BinaryOp::OR,
                                Box::new(Formula::recursive_simplify(a.as_ref().clone()).0),
                                Box::new(Formula::recursive_simplify(b.as_ref().clone()).0),
                            )),
                        ),
                        true,
                    ),
                    // No Immediate Simplification - Recursive Step
                    _ => {
                        let simplified_x = Formula::recursive_simplify(*x);
                        let simplified_y = Formula::recursive_simplify(*y);
                        (
                            Formula::BinaryExpression(
                                BinaryOp::AND,
                                Box::new(simplified_x.0),
                                Box::new(simplified_y.0),
                            ),
                            simplified_x.1 || simplified_y.1,
                        )
                    }
                }
            }
            Formula::BinaryExpression(BinaryOp::OR, x, y) => {
                match (x.as_ref(), y.as_ref()) {
                    // Annihilation Law: T ∨ ? ⊨ T
                    (Formula::Constant(true), _) => (Formula::Constant(true), true),
                    // Annihilation Law: ? ∨ T ⊨ T
                    (_, Formula::Constant(true)) => (Formula::Constant(true), true),
                    // Identity Law: ⊥ ∨ A ⊨ A
                    (Formula::Constant(false), _) => (Formula::recursive_simplify(*y).0, true),
                    // Identity Law: A ∨ ⊥ ⊨ A
                    (_, Formula::Constant(false)) => (Formula::recursive_simplify(*x).0, true),
                    // Law of Excluded Middle: ¬A ∨ A ⊨ T
                    (Formula::UnaryExpression(UnaryOp::NOT, a), _) if *a == y => {
                        (Formula::Constant(true), true)
                    }
                    // Law of Excluded Middle: A ∨ ¬A ⊨ T
                    (_, Formula::UnaryExpression(UnaryOp::NOT, b)) if x == *b => {
                        (Formula::Constant(true), true)
                    }
                    // Absorption Law: A ∨ (A ∧ ?) ⊨ A
                    (_, Formula::BinaryExpression(BinaryOp::AND, a, _)) if x == *a => {
                        (Formula::recursive_simplify(*x).0, true)
                    }
                    // Absorption Law: A ∨ (? ∧ A) ⊨ A
                    (_, Formula::BinaryExpression(BinaryOp::AND, _, b)) if x == *b => {
                        (Formula::recursive_simplify(*x).0, true)
                    }
                    // Absorption Law: (A ∧ ?) ∨ A ⊨ A
                    (Formula::BinaryExpression(BinaryOp::AND, a, _), _) if *a == y => {
                        (Formula::recursive_simplify(*y).0, true)
                    }
                    // Absorption Law: (? ∧ A) ∨ A ⊨ A
                    (Formula::BinaryExpression(BinaryOp::AND, _, b), _) if *b == y => {
                        (Formula::recursive_simplify(*y).0, true)
                    }
                    // Idempotency Law: A ∨ A ⊨ A
                    _ if x == y => (Formula::recursive_simplify(*x).0, true),
                    // De Morgan's Law: ¬A ∨ ¬B ⊨ ¬(A ∧ B)
                    (
                        Formula::UnaryExpression(UnaryOp::NOT, a),
                        Formula::UnaryExpression(UnaryOp::NOT, b),
                    ) => (
                        Formula::UnaryExpression(
                            UnaryOp::NOT,
                            Box::new(Formula::BinaryExpression(
                                BinaryOp::AND,
                                Box::new(Formula::recursive_simplify(a.as_ref().clone()).0),
                                Box::new(Formula::recursive_simplify(b.as_ref().clone()).0),
                            )),
                        ),
                        true,
                    ),
                    // No Immediate Simplification - Recursive Step
                    _ => {
                        let simplified_x = Formula::recursive_simplify(*x);
                        let simplified_y = Formula::recursive_simplify(*y);
                        (
                            Formula::BinaryExpression(
                                BinaryOp::OR,
                                Box::new(simplified_x.0),
                                Box::new(simplified_y.0),
                            ),
                            simplified_x.1 || simplified_y.1,
                        )
                    }
                }
            }
            Formula::BinaryExpression(BinaryOp::XOR, x, y) => {
                match (x.as_ref(), y.as_ref()) {
                    // Identity: A ⊕ ⊥ ⊨ A
                    (_, Formula::Constant(false)) => (Formula::recursive_simplify(*x).0, true),
                    // Identity: ⊥ ⊕ A ⊨ A
                    (Formula::Constant(false), _) => (Formula::recursive_simplify(*y).0, true),
                    // ¬A ⊕ A ⊨ T
                    (Formula::UnaryExpression(UnaryOp::NOT, a), _) if *a == y => {
                        (Formula::Constant(true), true)
                    }
                    // A ⊕ ¬A ⊨ T
                    (_, Formula::UnaryExpression(UnaryOp::NOT, b)) if x == *b => {
                        (Formula::Constant(true), true)
                    }
                    // A ⊕ T ⊨ ¬A
                    (_, Formula::Constant(true)) => {
                        let simplified_x = Formula::recursive_simplify(*x);
                        (
                            Formula::UnaryExpression(UnaryOp::NOT, Box::new(simplified_x.0)),
                            true,
                        )
                    }
                    // T ⊕ A ⊨ ¬A
                    (Formula::Constant(true), _) => {
                        let simplified_y = Formula::recursive_simplify(*y);
                        (
                            Formula::UnaryExpression(UnaryOp::NOT, Box::new(simplified_y.0)),
                            true,
                        )
                    }
                    // Self-inverse: A ⊕ A ⊨ ⊥
                    _ if x == y => (Formula::Constant(false), true),
                    // ¬A ⊕ ¬B ⊨ A ⊕ B
                    (
                        Formula::UnaryExpression(UnaryOp::NOT, a),
                        Formula::UnaryExpression(UnaryOp::NOT, b),
                    ) => (
                        Formula::BinaryExpression(
                            BinaryOp::XOR,
                            Box::new(Formula::recursive_simplify(a.as_ref().clone()).0),
                            Box::new(Formula::recursive_simplify(b.as_ref().clone()).0),
                        ),
                        true,
                    ),
                    // No Immediate Simplification - Recursive Step
                    _ => {
                        let simplified_x = Formula::recursive_simplify(*x);
                        let simplified_y = Formula::recursive_simplify(*y);
                        (
                            Formula::BinaryExpression(
                                BinaryOp::XOR,
                                Box::new(simplified_x.0),
                                Box::new(simplified_y.0),
                            ),
                            simplified_x.1 || simplified_y.1,
                        )
                    }
                }
            }
        }
    }

    fn may_hold(&self, tags: &ScalableCuckooFilter<TagId, DefaultHasher, ChaCha12Rng>) -> bool {
        match self {
            Formula::BinaryExpression(BinaryOp::AND, x, y) => {
                // Both tags must be present
                x.may_hold(tags) && y.may_hold(tags)
            }
            Formula::BinaryExpression(BinaryOp::OR, x, y) => {
                // At least one tag must be present
                x.may_hold(tags) || y.may_hold(tags)
            }
            Formula::BinaryExpression(BinaryOp::XOR, x, y) => {
                // At least one tag must be present
                x.may_hold(tags) || y.may_hold(tags)
            }
            Formula::UnaryExpression(UnaryOp::NOT, x) => {
                // We can guarantee there are no files for which the expression holds if and only if x is a Tautology
                !matches!(**x, Formula::Constant(true))
            }
            Formula::Constant(c) => *c, // Tautologies will always hold, Contradictions never will
            Formula::Proposition(p) => tags.contains(&p.tag_id), // The tag must be present
        }
    }
}

// TODO: define appropriate errors, include I/O, etc.
#[derive(Debug)]
pub enum QueryErr {
    SyntaxError,               // The Query is incorrectly formatted
    ParseError,                // A Tag_ID is not a valid UUID
    RuntimeError(RetrieveErr), // The Query could not be executed
}

//[!] Wrapper for a cacheservice.call() ?

#[derive(Debug)]
pub enum RetrieveErr {
    CacheError,
    TagParseError,
}
