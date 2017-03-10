use error::*;
use std::collections::BTreeMap;

type ChildMap<N> = BTreeMap<String, N>;

pub trait HandleReader<H, N>: ReadWalkable<N> {
    fn read_shallow(&mut self, handle: H) -> Result<N>;

    fn walk_handle<O>(&mut self, op: &mut O, start: H) -> Result<O::VisitResult>
        where O: WalkOp<N>
    {
        let first = self.read_shallow(start)?;
        self.walk_node(op, first)?.ok_or("No answer".into())
    }
}

pub trait ReadWalkable<N> {
    fn read_children(&mut self, node: &N) -> Result<ChildMap<N>>;

    fn walk_node<O>(&mut self,
                    op: &mut O,
                    node: N)
                    -> Result<Option<O::VisitResult>>
        where O: WalkOp<N>
    {
        if op.should_descend(&node) {
            let mut children = ChildMap::new();
            for (name, node) in self.read_children(&node)? {
                if let Some(result) = self.walk_node(op, node)? {
                    children.insert(name, result);
                }
            }
            op.post_descend(node, children)
        } else {
            op.no_descend(node)
        }
    }
}

pub trait HasChildMap: Sized {
    fn child_map(&self) -> Option<&ChildMap<Self>>;

    fn walk<'s, 'o, O>(&'s self,
                       op: &'o mut O)
                       -> Result<Option<O::VisitResult>>
        where O: WalkOp<&'s Self>
    {
        ().walk_node(op, &self)
    }
}

impl<'a, N: 'a> ReadWalkable<&'a N> for ()
    where N: HasChildMap
{
    fn read_children(&mut self, node: &&'a N) -> Result<ChildMap<&'a N>> {
        let mut children = ChildMap::new();
        if let Some(mykids) = node.child_map() {
            for (name, node) in mykids {
                children.insert(name.to_owned(), node);
            }
        }
        Ok(children)
    }
}

pub trait WalkOp<N> {
    type VisitResult;

    fn should_descend(&mut self, node: &N) -> bool;
    fn no_descend(&mut self, node: N) -> Result<Option<Self::VisitResult>>;
    fn post_descend(&mut self,
                    node: N,
                    children: ChildMap<Self::VisitResult>)
                    -> Result<Option<Self::VisitResult>>;
}
