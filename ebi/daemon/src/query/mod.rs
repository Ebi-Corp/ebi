pub mod file_order;
use crate::query::file_order::{FileOrder, OrderedFileSummary};
use crate::services::query::Retriever;
use crate::tag::TagId;
use rand_chacha::ChaCha12Rng;
use scalable_cuckoo_filter::{DefaultHasher, ScalableCuckooFilter};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Formula {
    Proposition(Proposition),
    BinaryExpression(BinaryOp, Box<Formula>, Box<Formula>),
    UnaryExpression(UnaryOp, Box<Formula>),
}

impl Formula {
    fn get_tags(&self) -> HashSet<TagId> {
        match self {
            Formula::BinaryExpression(_, x, y) => x
                .get_tags()
                .union(&y.get_tags())
                .cloned()
                .collect::<HashSet<TagId>>(),
            Formula::UnaryExpression(_, x) => x.get_tags(),
            Formula::Proposition(p) => HashSet::from([p.tag_id]),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum BinaryOp {
    AND,
    OR,
    XOR,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
enum UnaryOp {
    NOT,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone, Serialize, Deserialize)]
struct Proposition {
    //[!] Allow for tag_id to be empty, so we can represent Tautologies/Contradictions
    tag_id: TagId,
}

//[/] Own implementation of decode can enforce that BTreeSet contains files of the specified FileOrder
#[derive(Serialize, Deserialize, Clone)]
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

    pub async fn evaluate(
        //[/] Only local shelves
        &mut self,
        retriever: Retriever,
    ) -> Result<BTreeSet<OrderedFileSummary>, QueryErr> {
        Query::recursive_evaluate(self.formula.clone(), Arc::new(retriever)).await
    }

    async fn recursive_evaluate(
        formula: Formula,
        ret_srv: Arc<Retriever>,
    ) -> Result<BTreeSet<OrderedFileSummary>, QueryErr> {
        match formula {
            Formula::BinaryExpression(BinaryOp::AND, x, y) => match (*x.clone(), *y.clone()) {
                (_, Formula::UnaryExpression(UnaryOp::NOT, b)) => {
                    let a =
                        Box::pin(Query::recursive_evaluate(*x.clone(), ret_srv.clone())).await?;
                    let b =
                        Box::pin(Query::recursive_evaluate(*b.clone(), ret_srv.clone())).await?;
                    let x: BTreeSet<OrderedFileSummary> = a.difference(&b).cloned().collect();
                    Ok(x)
                }
                (Formula::UnaryExpression(UnaryOp::NOT, a), _) => {
                    let a =
                        Box::pin(Query::recursive_evaluate(*a.clone(), ret_srv.clone())).await?;
                    let b =
                        Box::pin(Query::recursive_evaluate(*y.clone(), ret_srv.clone())).await?;
                    let x: BTreeSet<OrderedFileSummary> = b.difference(&a).cloned().collect();
                    Ok(x)
                }
                (a, b) => {
                    let a = Box::pin(Query::recursive_evaluate(a.clone(), ret_srv.clone())).await?;
                    let b = Box::pin(Query::recursive_evaluate(b.clone(), ret_srv.clone())).await?;
                    let x: BTreeSet<OrderedFileSummary> = a.intersection(&b).cloned().collect();
                    Ok(x)
                }
            },
            Formula::BinaryExpression(BinaryOp::OR, x, y) => {
                let a = Box::pin(Query::recursive_evaluate(*x.clone(), ret_srv.clone())).await?;
                let b = Box::pin(Query::recursive_evaluate(*y.clone(), ret_srv.clone())).await?;
                let x: BTreeSet<OrderedFileSummary> = a.union(&b).cloned().collect();
                Ok(x)
            }
            Formula::BinaryExpression(BinaryOp::XOR, x, y) => {
                let a = Box::pin(Query::recursive_evaluate(*x.clone(), ret_srv.clone())).await?;
                let b = Box::pin(Query::recursive_evaluate(*y.clone(), ret_srv.clone())).await?;
                let x: BTreeSet<OrderedFileSummary> = a.symmetric_difference(&b).cloned().collect();
                Ok(x)
            }
            Formula::UnaryExpression(UnaryOp::NOT, x) => {
                let a = ret_srv.get_all().await.map_err(QueryErr::RuntimeError)?;
                let b = Box::pin(Query::recursive_evaluate(*x.clone(), ret_srv)).await?;
                let x: BTreeSet<OrderedFileSummary> = a.difference(&b).cloned().collect();
                Ok(x)
            }
            Formula::Proposition(p) => ret_srv.get(p.tag_id).await.map_err(QueryErr::RuntimeError),
        }
    }

    fn simplify(&mut self) {
        loop {
            let simplified_formula = Formula::recursive_simplify(self.formula.clone());
            self.formula = simplified_formula.0;
            if simplified_formula.1 {
                break;
            }
        }
    }
}

impl Formula {
    fn recursive_simplify(formula: Formula) -> (Formula, bool) {
        // Further simplification is possible but NP-Hard
        match formula {
            Formula::Proposition(_) => (formula, false),
            Formula::BinaryExpression(BinaryOp::AND, x, y) => match *x.clone() {
                Formula::Proposition(p) => match *y {
                    Formula::BinaryExpression(BinaryOp::OR, a, b) => {
                        // Absorption Law: A AND (A OR ?) ⊨ A
                        if let Formula::Proposition(Proposition { tag_id }) = *a {
                            if tag_id == p.tag_id {
                                return (
                                    Formula::Proposition(Proposition { tag_id: p.tag_id }),
                                    true,
                                );
                            }
                        }
                        // Absorption Law: A AND (? OR A) ⊨ A
                        if let Formula::Proposition(Proposition { tag_id }) = *b {
                            if tag_id == p.tag_id {
                                return (
                                    Formula::Proposition(Proposition { tag_id: p.tag_id }),
                                    true,
                                );
                            }
                        }
                        let simplified_a = Formula::recursive_simplify(*a.clone());
                        let simplified_b = Formula::recursive_simplify(*b.clone());
                        (
                            Formula::BinaryExpression(
                                BinaryOp::AND,
                                Box::new(Formula::Proposition(p)),
                                Box::new(Formula::BinaryExpression(
                                    BinaryOp::OR,
                                    Box::new(simplified_a.0),
                                    Box::new(simplified_b.0),
                                )),
                            ),
                            simplified_a.1 || simplified_b.1,
                        )
                    }
                    // Idempotency Law: A AND A ⊨ A
                    Formula::Proposition(Proposition { tag_id }) => {
                        if tag_id == p.tag_id {
                            return (Formula::Proposition(Proposition { tag_id: p.tag_id }), true);
                        }
                        let simplified_y = Formula::recursive_simplify(*y.clone());
                        (
                            Formula::BinaryExpression(
                                BinaryOp::OR,
                                Box::new(Formula::Proposition(p)),
                                Box::new(simplified_y.0),
                            ),
                            simplified_y.1,
                        )
                    }
                    _ => {
                        let simplified_x = Formula::recursive_simplify(*x.clone());
                        let simplified_y = Formula::recursive_simplify(*y.clone());
                        (
                            Formula::BinaryExpression(
                                BinaryOp::AND,
                                Box::new(simplified_x.0),
                                Box::new(simplified_y.0),
                            ),
                            simplified_x.1 || simplified_y.1,
                        )
                    }
                },
                Formula::BinaryExpression(BinaryOp::OR, a, b) => match *y {
                    Formula::Proposition(p) => {
                        // Absorption Law: (A OR ?) AND A ⊨ A
                        if let Formula::Proposition(Proposition { tag_id }) = *a {
                            if tag_id == p.tag_id {
                                return (
                                    Formula::Proposition(Proposition { tag_id: p.tag_id }),
                                    true,
                                );
                            }
                        }
                        // Absorption Law: (? OR A) AND A ⊨ A
                        if let Formula::Proposition(Proposition { tag_id }) = *b {
                            if tag_id == p.tag_id {
                                return (
                                    Formula::Proposition(Proposition { tag_id: p.tag_id }),
                                    true,
                                );
                            }
                        }
                        let simplified_a = Formula::recursive_simplify(*a.clone());
                        let simplified_b = Formula::recursive_simplify(*b.clone());
                        (
                            Formula::BinaryExpression(
                                BinaryOp::AND,
                                Box::new(Formula::BinaryExpression(
                                    BinaryOp::OR,
                                    Box::new(simplified_a.0),
                                    Box::new(simplified_b.0),
                                )),
                                Box::new(Formula::Proposition(p)),
                            ),
                            simplified_a.1 || simplified_b.1,
                        )
                    }
                    _ => {
                        let simplified_x = Formula::recursive_simplify(*x.clone());
                        let simplified_y = Formula::recursive_simplify(*y.clone());
                        (
                            Formula::BinaryExpression(
                                BinaryOp::AND,
                                Box::new(simplified_x.0),
                                Box::new(simplified_y.0),
                            ),
                            simplified_x.1 || simplified_y.1,
                        )
                    }
                },
                // De Morgan's Law: (NOT A) AND (NOT B) ⊨ NOT (A OR B)
                Formula::UnaryExpression(UnaryOp::NOT, a) => match *y {
                    Formula::UnaryExpression(UnaryOp::NOT, b) => (
                        Formula::UnaryExpression(
                            UnaryOp::NOT,
                            Box::new(Formula::BinaryExpression(
                                BinaryOp::OR,
                                Box::new(Formula::recursive_simplify(*a).0),
                                Box::new(Formula::recursive_simplify(*b).0),
                            )),
                        ),
                        true,
                    ),
                    _ => {
                        let simplified_x = Formula::recursive_simplify(*x.clone());
                        let simplified_y = Formula::recursive_simplify(*y.clone());
                        (
                            Formula::BinaryExpression(
                                BinaryOp::AND,
                                Box::new(simplified_x.0),
                                Box::new(simplified_y.0),
                            ),
                            simplified_x.1 || simplified_y.1,
                        )
                    }
                },
                _ => {
                    let simplified_x = Formula::recursive_simplify(*x.clone());
                    let simplified_y = Formula::recursive_simplify(*y.clone());
                    (
                        Formula::BinaryExpression(
                            BinaryOp::AND,
                            Box::new(simplified_x.0),
                            Box::new(simplified_y.0),
                        ),
                        simplified_x.1 || simplified_y.1,
                    )
                }
            },
            Formula::BinaryExpression(BinaryOp::OR, x, y) => match *x.clone() {
                Formula::Proposition(p) => match *y {
                    Formula::BinaryExpression(BinaryOp::AND, a, b) => {
                        // Absorption Law: A OR (A AND ?) ⊨ A
                        if let Formula::Proposition(Proposition { tag_id }) = *a {
                            if tag_id == p.tag_id {
                                return (
                                    Formula::Proposition(Proposition { tag_id: p.tag_id }),
                                    true,
                                );
                            }
                        }
                        // Absorption Law: A OR (? AND A) ⊨ A
                        if let Formula::Proposition(Proposition { tag_id }) = *b {
                            if tag_id == p.tag_id {
                                return (
                                    Formula::Proposition(Proposition { tag_id: p.tag_id }),
                                    true,
                                );
                            }
                        }
                        let simplified_a = Formula::recursive_simplify(*a.clone());
                        let simplified_b = Formula::recursive_simplify(*b.clone());
                        (
                            Formula::BinaryExpression(
                                BinaryOp::OR,
                                Box::new(Formula::Proposition(p)),
                                Box::new(Formula::BinaryExpression(
                                    BinaryOp::AND,
                                    Box::new(simplified_a.0),
                                    Box::new(simplified_b.0),
                                )),
                            ),
                            simplified_a.1 || simplified_b.1,
                        )
                    }
                    // Idempotency Law: A OR A ⊨ A
                    Formula::Proposition(Proposition { tag_id }) => {
                        if tag_id == p.tag_id {
                            return (Formula::Proposition(Proposition { tag_id: p.tag_id }), true);
                        }
                        let simplified_y = Formula::recursive_simplify(*y.clone());
                        (
                            Formula::BinaryExpression(
                                BinaryOp::OR,
                                Box::new(Formula::Proposition(p)),
                                Box::new(simplified_y.0),
                            ),
                            simplified_y.1,
                        )
                    }
                    _ => {
                        let simplified_x = Formula::recursive_simplify(*x.clone());
                        let simplified_y = Formula::recursive_simplify(*y.clone());
                        (
                            Formula::BinaryExpression(
                                BinaryOp::OR,
                                Box::new(simplified_x.0),
                                Box::new(simplified_y.0),
                            ),
                            simplified_x.1 || simplified_y.1,
                        )
                    }
                },
                Formula::BinaryExpression(BinaryOp::AND, a, b) => match *y {
                    Formula::Proposition(p) => {
                        // Absorption Law: (A AND ?) OR A ⊨ A
                        if let Formula::Proposition(Proposition { tag_id }) = *a {
                            if tag_id == p.tag_id {
                                return (
                                    Formula::Proposition(Proposition { tag_id: p.tag_id }),
                                    true,
                                );
                            }
                        }
                        // Absorption Law: (? AND A) OR A ⊨ A
                        if let Formula::Proposition(Proposition { tag_id }) = *b {
                            if tag_id == p.tag_id {
                                return (
                                    Formula::Proposition(Proposition { tag_id: p.tag_id }),
                                    true,
                                );
                            }
                        }
                        let simplified_a = Formula::recursive_simplify(*a.clone());
                        let simplified_b = Formula::recursive_simplify(*b.clone());
                        (
                            Formula::BinaryExpression(
                                BinaryOp::OR,
                                Box::new(Formula::BinaryExpression(
                                    BinaryOp::AND,
                                    Box::new(simplified_a.0),
                                    Box::new(simplified_b.0),
                                )),
                                Box::new(Formula::Proposition(p)),
                            ),
                            simplified_a.1 || simplified_b.1,
                        )
                    }
                    _ => {
                        let simplified_x = Formula::recursive_simplify(*x.clone());
                        let simplified_y = Formula::recursive_simplify(*y.clone());
                        (
                            Formula::BinaryExpression(
                                BinaryOp::OR,
                                Box::new(simplified_x.0),
                                Box::new(simplified_y.0),
                            ),
                            simplified_x.1 || simplified_y.1,
                        )
                    }
                },
                // De Morgan's Law: (NOT A) OR (NOT B) ⊨ NOT (A AND B)
                Formula::UnaryExpression(UnaryOp::NOT, a) => match *y {
                    Formula::UnaryExpression(UnaryOp::NOT, b) => (
                        Formula::UnaryExpression(
                            UnaryOp::NOT,
                            Box::new(Formula::BinaryExpression(
                                BinaryOp::AND,
                                Box::new(Formula::recursive_simplify(*a).0),
                                Box::new(Formula::recursive_simplify(*b).0),
                            )),
                        ),
                        true,
                    ),
                    _ => {
                        let simplified_x = Formula::recursive_simplify(*x.clone());
                        let simplified_y = Formula::recursive_simplify(*y.clone());
                        (
                            Formula::BinaryExpression(
                                BinaryOp::OR,
                                Box::new(simplified_x.0),
                                Box::new(simplified_y.0),
                            ),
                            simplified_x.1 || simplified_y.1,
                        )
                    }
                },
                _ => {
                    let simplified_x = Formula::recursive_simplify(*x.clone());
                    let simplified_y = Formula::recursive_simplify(*y.clone());
                    (
                        Formula::BinaryExpression(
                            BinaryOp::OR,
                            Box::new(simplified_x.0),
                            Box::new(simplified_y.0),
                        ),
                        simplified_x.1 || simplified_y.1,
                    )
                }
            },
            Formula::BinaryExpression(BinaryOp::XOR, x, y) => match *x.clone() {
                // (NOT A) XOR (NOT B) ⊨ A XOR B
                Formula::UnaryExpression(UnaryOp::NOT, a) => match *y {
                    Formula::UnaryExpression(UnaryOp::NOT, b) => (
                        Formula::BinaryExpression(
                            BinaryOp::XOR,
                            Box::new(Formula::recursive_simplify(*a).0),
                            Box::new(Formula::recursive_simplify(*b).0),
                        ),
                        true,
                    ),
                    _ => {
                        let simplified_x = Formula::recursive_simplify(*x.clone());
                        let simplified_y = Formula::recursive_simplify(*y.clone());
                        (
                            Formula::BinaryExpression(
                                BinaryOp::XOR,
                                Box::new(simplified_x.0),
                                Box::new(simplified_y.0),
                            ),
                            simplified_x.1 || simplified_y.1,
                        )
                    }
                },
                _ => {
                    let simplified_x = Formula::recursive_simplify(*x.clone());
                    let simplified_y = Formula::recursive_simplify(*y.clone());
                    (
                        Formula::BinaryExpression(
                            BinaryOp::XOR,
                            Box::new(simplified_x.0),
                            Box::new(simplified_y.0),
                        ),
                        simplified_x.1 || simplified_y.1,
                    )
                }
            },
            Formula::UnaryExpression(UnaryOp::NOT, x) => match *x {
                // Double Negation: NOT (NOT A) ⊨ A
                Formula::UnaryExpression(UnaryOp::NOT, y) => {
                    (Formula::recursive_simplify(*y).0, true)
                }
                _ => {
                    let simplified_x = Formula::recursive_simplify(*x.clone());
                    (
                        Formula::UnaryExpression(UnaryOp::NOT, Box::new(simplified_x.0)),
                        simplified_x.1,
                    )
                }
            },
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
            Formula::UnaryExpression(UnaryOp::NOT, _) => {
                // The tag may be present
                /*
                This should actually return false if the expression under the NOT is a tautology
                Tautology detection can be reduced to the SAT problem (φ is a tautology iff ¬φ is NOT satisfiable)
                SAT has been proven to be NP-Complete
                Not really worth the effort for a network optimisation heuristic
                */
                true
            }
            Formula::Proposition(p) => tags.contains(&p.tag_id), // The tag must be present
        }
    }
}

// TODO: define appropriate errors, include I/O, etc.
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
