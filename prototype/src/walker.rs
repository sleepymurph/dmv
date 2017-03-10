use error::*;
use std::collections::BTreeMap;

type ChildMap<N> = BTreeMap<String, N>;

pub trait ReadWalkable<H, N> {
    fn read_shallow(&mut self, handle: H) -> Result<N>;
    fn read_children(&mut self, node: &N) -> Result<ChildMap<N>>;
}

pub trait HasChildMap: Sized {
    fn child_map(&self) -> Option<&ChildMap<Self>>;

    fn walk<'s, 'o, O>(&'s self,
                       op: &'o mut O)
                       -> Result<Option<O::VisitResult>>
        where O: WalkOp<&'s Self>
    {
        if op.should_descend(&self) {
            let mut children = ChildMap::new();
            if let Some(mykids) = self.child_map() {
                for (name, node) in mykids {
                    if let Some(result) = node.walk(op)? {
                        children.insert(name.to_owned(), result);
                    }
                }
            }
            op.post_descend(self, children)
        } else {
            op.no_descend(self)
        }
    }
}

impl<'a, N> ReadWalkable<&'a N, &'a N> for ()
    where N: HasChildMap
{
    fn read_shallow(&mut self, handle: &'a N) -> Result<&'a N> { Ok(handle) }
    fn read_children(&mut self, node: &&'a N) -> Result<ChildMap<&'a N>> {
        let mut translated = ChildMap::new();
        if let Some(map) = node.child_map() {
            for (name, child) in map {
                translated.insert(name.to_owned(), child);
            }
        }
        Ok(translated)
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

pub fn walk<H, N, R, O>(reader: &mut R,
                        op: &mut O,
                        start: H)
                        -> Result<O::VisitResult>
    where R: ReadWalkable<H, N>,
          O: WalkOp<N>
{
    let first = reader.read_shallow(start)?;
    walk_inner(reader, op, first)?.ok_or("No answer".into())
}

fn walk_inner<H, N, R, O>(reader: &mut R,
                          op: &mut O,
                          node: N)
                          -> Result<Option<O::VisitResult>>
    where R: ReadWalkable<H, N>,
          O: WalkOp<N>
{
    if op.should_descend(&node) {
        let mut children = ChildMap::new();
        for (name, node) in reader.read_children(&node)? {
            if let Some(result) = walk_inner(reader, op, node)? {
                children.insert(name, result);
            }
        }
        op.post_descend(node, children)
    } else {
        op.no_descend(node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(PartialEq,Debug)]
    enum DeepNode<C, L, T> {
        Leaf { common: C, leaf: L },
        Tree {
            common: C,
            tree: T,
            children: ChildMap<DeepNode<C, L, T>>,
        },
    }

    impl<C, L, T> DeepNode<C, L, T> {
        fn _leaf(common: C, leaf: L) -> Self {
            DeepNode::Leaf {
                common: common,
                leaf: leaf,
            }
        }
        fn tree(common: C, tree: T) -> Self {
            DeepNode::Tree {
                common: common,
                tree: tree,
                children: ChildMap::new(),
            }
        }
        fn _with_children(common: C,
                          tree: T,
                          children: ChildMap<Self>)
                          -> Self {
            DeepNode::Tree {
                common: common,
                tree: tree,
                children: children,
            }
        }
    }

    impl<C, L, T> HasChildMap for DeepNode<C, L, T> {
        fn child_map(&self) -> Option<&ChildMap<Self>> {
            match self {
                &DeepNode::Leaf { .. } => None,
                &DeepNode::Tree { ref children, .. } => Some(children),
            }
        }
    }

    type DummyDeepNode = DeepNode<String, (), ()>;

    #[test]
    fn it_works() {
        let node = DummyDeepNode::tree("Hello".into(), ());
        let read_shallow = ().read_shallow(&node);
        assert_match!(read_shallow, Ok(read_node) if read_node==&node);
    }
}
