//! A framework for walking different hierarchies of nodes

use error::*;
use maputil::mux;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Type for reading and iterating over a node's children
pub type ChildMap<N> = BTreeMap<String, N>;

/// Tracks the position in the hierarchy during a walk
pub type PathStack = PathBuf;



/// A repository that can look up nodes by some handle
pub trait NodeLookup<H, N>: NodeReader<N> {
    /// Given a handlem, read in the appropriate node
    fn lookup_node(&self, handle: H) -> Result<N>;

    /// Do a walk operation, starting with the given node handle
    fn walk_handle<O>(&self,
                      op: &mut O,
                      start: H)
                      -> Result<Option<O::VisitResult>>
        where O: WalkOp<N>
    {
        let first = self.lookup_node(start)?;
        self.walk_node(op, first)
    }
}


/// A repository that can follow from one node to get its children
pub trait NodeReader<N> {
    /// Given a node, read its children
    fn read_children(&self, node: &N) -> Result<ChildMap<N>>;

    /// Do a walk operation, starting with the given node
    fn walk_node<O>(&self,
                    op: &mut O,
                    node: N)
                    -> Result<Option<O::VisitResult>>
        where O: WalkOp<N>
    {
        self.walk_node_stack(op, node, &mut PathStack::new())
    }

    /// Do a walk operation, tracking the path stack
    ///
    /// This is the inner function of the recursion
    fn walk_node_stack<O>(&self,
                          op: &mut O,
                          node: N,
                          path_stack: &mut PathStack)
                          -> Result<Option<O::VisitResult>>
        where O: WalkOp<N>
    {
        if op.should_descend(path_stack, &node) {
            trace!("-> {}", path_stack.display());
            op.pre_descend(path_stack, &node)?;
            let mut children = ChildMap::new();
            for (name, node) in self.read_children(&node)? {
                path_stack.push(name.to_owned());
                if let Some(result) =
                       self.walk_node_stack(op, node, path_stack)? {
                    children.insert(name, result);
                }
                path_stack.pop();
            }
            trace!("<- {}", path_stack.display());
            op.post_descend(path_stack, node, children)
        } else {
            trace!("** {}", path_stack.display());
            op.no_descend(path_stack, node)
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
    fn read_children(&self, node: &&'a N) -> Result<ChildMap<&'a N>> {
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
    fn should_descend(&mut self, path_stack: &PathStack, node: &N) -> bool;

    /// Called before visiting a node that was not descended into
    ///
    /// Return None to not include the result in the list of children
    ///
    /// Default implementation is a no-op
    fn no_descend(&mut self,
                  _ps: &PathStack,
                  _node: N)
                  -> Result<Option<Self::VisitResult>> {
        Ok(None)
    }


    /// Called before descending in to tree node to gather its child results
    ///
    /// Default implementation is a no-op
    fn pre_descend(&mut self, _ps: &PathStack, _node: &N) -> Result<()> {
        Ok(())
    }

    /// Called after descending in to tree node and gathering its child results
    ///
    /// Default implementation is a no-op
    fn post_descend(&mut self,
                    _ps: &PathStack,
                    _node: N,
                    _children: ChildMap<Self::VisitResult>)
                    -> Result<Option<Self::VisitResult>> {
        Ok(None)
    }
}


impl<'a, A, B, RA, RB> NodeReader<(Option<A>, Option<B>)> for (&'a RA, &'a RB)
    where RA: NodeReader<A>,
          RB: NodeReader<B>
{
    fn read_children(&self,
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



impl<'a, A, RA> NodeReader<Vec<Option<A>>> for RA
    where RA: NodeReader<A>
{
    fn read_children(&self,
                     node: &Vec<Option<A>>)
                     -> Result<ChildMap<Vec<Option<A>>>> {

        // optional childmaps
        let mut ocms: Vec<Option<ChildMap<A>>> = Vec::with_capacity(node.len());
        for n in node {
            let ocm = n.as_ref().and_then_try(|n| self.read_children(n))?;
            ocms.push(ocm);
        }

        // children iterators
        let mut cis = Vec::with_capacity(node.len());
        for ocm in ocms {
            let ci = ocm.into_iter().flat_map(|cm| cm.into_iter()).peekable();
            cis.push(ci);
        }

        // chilren
        let mut children = ChildMap::new();

        // multiplex the children
        while let Some(next_name) = cis.iter_mut()
                .map(|ci| ci.peek())            // each next child (option)
                .flat_map(|oc| oc.into_iter())  // unwrap each Some(next child)
                .map(|&(ref name, _)| name)     // each name
                .min()                          // minimum name
                .map(|name| name.to_owned())    // release the borrow
        {

            // child row
            let mut cs = Vec::with_capacity(node.len());
            for ci in cis.iter_mut() {
                if ci.peek().map(|&(ref name,_)| name) == Some(&next_name) {
                    cs.push(ci.next().map(|(_,a)| a));
                } else {
                    cs.push(None);
                }
            }
            children.insert(next_name, cs);
        }
        Ok(children)
    }
}



impl<'a, A, B, RA, RB> NodeReader<(Vec<Option<A>>, Option<B>)>
    for (&'a RA, &'a RB)
    where RA: NodeReader<Vec<Option<A>>>,
          RB: NodeReader<B>,
          A: Clone
{
    fn read_children(&self,
                     node: &(Vec<Option<A>>, Option<B>))
                     -> Result<ChildMap<(Vec<Option<A>>, Option<B>)>> {
        let count_a = node.0.len();
        let mut children = ChildMap::new();
        match node {
            &(ref a, Some(ref b)) => {
                let a: ChildMap<Vec<Option<A>>> = self.0.read_children(a)?;
                let b: ChildMap<B> = self.1.read_children(b)?;
                for (name, a, b) in mux(a.into_iter(), b.into_iter()) {
                    let a = a.unwrap_or_else(|| vec![None;count_a]);
                    children.insert(name, (a, b));
                }
            }
            &(ref a, None) => {
                for (name, a) in self.0.read_children(a)? {
                    children.insert(name, (a, None));
                }
            }
        }
        Ok(children)
    }
}
