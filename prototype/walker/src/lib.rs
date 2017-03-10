use std::collections::BTreeMap;

pub trait Walkable {
    fn is_tree(&self) -> bool;
}

type Result<T> = ::std::result::Result<T, String>;

type ChildMap<N> = BTreeMap<String, N>;

pub trait ReadWalkable<H, N: Walkable> {
    type Iter: Iterator<Item = Result<(String, N)>>;

    fn read_shallow(&mut self, handle: H) -> Result<N>;
    fn read_children(&mut self, node: &N) -> Result<Self::Iter>;
}

pub trait WalkOp<N: Walkable> {
    type PostResult;

    fn visit_leaf(&mut self, node: N) -> Result<Option<Self::PostResult>>;
    fn enter_tree(&mut self, node: &N) -> Result<bool>;
    fn leave_tree(&mut self,
                  node: N,
                  children: ChildMap<Self::PostResult>)
                  -> Result<Option<Self::PostResult>>;
}

pub fn walk<H, N, R, O>(reader: &mut R,
                        op: &mut O,
                        start: H)
                        -> Result<O::PostResult>
    where N: Walkable,
          R: ReadWalkable<H, N>,
          O: WalkOp<N>
{
    let first = reader.read_shallow(start)?;
    walk_inner(reader, op, first)?.ok_or("No answer".into())
}

fn walk_inner<H, N, R, O>(reader: &mut R,
                          op: &mut O,
                          node: N)
                          -> Result<Option<O::PostResult>>
    where N: Walkable,
          R: ReadWalkable<H, N>,
          O: WalkOp<N>
{
    if node.is_tree() {
        let descend = op.enter_tree(&node)?;
        let mut children = ChildMap::new();
        if descend {
            for child in reader.read_children(&node)? {
                let (name, node) = child?;
                if let Some(result) = walk_inner(reader, op, node)? {
                    children.insert(name, result);
                }
            }
        }
        op.leave_tree(node, children)
    } else {
        op.visit_leaf(node)
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
        fn leaf(common: C, leaf: L) -> Self {
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
        fn with_children(common: C, tree: T, children: ChildMap<Self>) -> Self {
            DeepNode::Tree {
                common: common,
                tree: tree,
                children: children,
            }
        }
    }

    impl<C, L, T> Walkable for DeepNode<C, L, T> {
        fn is_tree(&self) -> bool {
            match self {
                &DeepNode::Leaf { .. } => false,
                &DeepNode::Tree { .. } => true,
            }
        }
    }

    impl<'a, C, L, T> Walkable for &'a DeepNode<C, L, T> {
        fn is_tree(&self) -> bool { self.is_tree() }
    }

    type DummyDeepNode = DeepNode<String, (), ()>;

    impl<'a> ReadWalkable<&'a DummyDeepNode, &'a DummyDeepNode> for () {
        type Iter = Box<Iterator<Item = Result<(String, &'a DummyDeepNode)>>+'a>;

        fn read_shallow(&mut self,
                        handle: &'a DummyDeepNode)
                        -> Result<&'a DummyDeepNode> {
            Ok(handle)
        }
        fn read_children(&mut self,
                         node: &&'a DummyDeepNode)
                         -> Result<Self::Iter> {
            match *node {
                &DeepNode::Leaf { .. } => Err("not a tree".into()),
                &DeepNode::Tree { ref children, .. } => {
                    Ok(Box::new(children.into_iter()
                        .map(|(s, n)| Ok((s.clone(), n)))))
                }
            }
        }
    }

    #[test]
    fn it_works() {
        let node = DummyDeepNode::tree("Hello".into(), ());
        let read_shallow = ().read_shallow(&node);
        assert_eq!(read_shallow, Ok(&node));
    }
}
