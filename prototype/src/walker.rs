//! A framework for walking different hierarchies of nodes

use error::*;
use maputil::mux;
use std::collections::BTreeMap;

/// Type for reading and iterating over a node's children
pub type ChildMap<N> = BTreeMap<String, N>;

/// A repository that can look up nodes by some handle
pub trait NodeLookup<H, N>: NodeReader<N> {
    /// Given a handlem, read in the appropriate node
    fn lookup_node(&mut self, handle: H) -> Result<N>;

    /// Do a walk operation, starting with the given node handle
    fn walk_handle<O>(&mut self, op: &mut O, start: H) -> Result<O::VisitResult>
        where O: WalkOp<N>
    {
        let first = self.lookup_node(start)?;
        self.walk_node(op, first)?.ok_or("No answer".into())
    }
}

/// A repository that can follow from one node to get its children
pub trait NodeReader<N> {
    /// Given a node, read its children
    fn read_children(&mut self, node: &N) -> Result<ChildMap<N>>;

    /// Do a walk operation, starting with the given node
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

/// A node type that has its children in memory, can be walked directly
pub trait NodeWithChildren: Sized {
    /// Return the node's children
    fn children(&self) -> Option<&ChildMap<Self>>;

    /// Do a walk operation, starting with this node
    fn walk<'s, 'o, O>(&'s self,
                       op: &'o mut O)
                       -> Result<Option<O::VisitResult>>
        where O: WalkOp<&'s Self>
    {
        ().walk_node(op, &self)
    }
}

/// Implement NodeReader for any NodeWithChildren, so it can be its own reader
impl<'a, N: 'a> NodeReader<&'a N> for ()
    where N: NodeWithChildren
{
    fn read_children(&mut self, node: &&'a N) -> Result<ChildMap<&'a N>> {
        let mut children = ChildMap::new();
        if let Some(mykids) = node.children() {
            for (name, node) in mykids {
                children.insert(name.to_owned(), node);
            }
        }
        Ok(children)
    }
}

/// An operation that takes place by walking over nodes
pub trait WalkOp<N> {
    /// The result of this operation
    type VisitResult;

    /// Called before descending into a tree node, return false to stop descent
    fn should_descend(&mut self, node: &N) -> bool;

    /// Called before visiting a node that was not descended into
    ///
    /// Return None to not include the result in the list of children
    fn no_descend(&mut self, node: N) -> Result<Option<Self::VisitResult>>;

    /// Called after descending in to tree node and gathering its child results
    fn post_descend(&mut self,
                    node: N,
                    children: ChildMap<Self::VisitResult>)
                    -> Result<Option<Self::VisitResult>>;
}


impl<A, B, RA, RB> NodeReader<(Option<A>, Option<B>)> for (RA, RB)
    where RA: NodeReader<A>,
          RB: NodeReader<B>
{
    fn read_children(&mut self,
                     node: &(Option<A>, Option<B>))
                     -> Result<ChildMap<(Option<A>, Option<B>)>> {
        let mut children = ChildMap::new();
        match node {
            &(Some(ref a), Some(ref b)) => {
                let a = self.0.read_children(a)?;
                let b = self.1.read_children(b)?;
                for (name, a, b) in mux(a.into_iter(), b.into_iter()) {
                    children.insert(name, (a, b));
                }
            }
            &(Some(ref a), None) => {
                for (name, a) in self.0.read_children(a)? {
                    children.insert(name, (Some(a), None));
                }
            }
            &(None, Some(ref b)) => {
                for (name, b) in self.1.read_children(b)? {
                    children.insert(name, (None, Some(b)));
                }
            }
            &(None, None) => {}
        }
        Ok(children)
    }
}
