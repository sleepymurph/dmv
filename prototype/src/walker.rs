use error::*;
use std::collections::BTreeMap;

type ChildMap<N> = BTreeMap<String, N>;

pub trait ReadWalkable<H, N> {
    type Iter: Iterator<Item = Result<(String, N)>>;

    fn read_shallow(&mut self, handle: H) -> Result<N>;
    fn read_children(&mut self, node: &N) -> Result<Self::Iter>;
}

pub trait WalkOp<N> {
    type VisitResult;

    fn should_descend(&mut self, node: &N) -> Result<bool>;
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
    if op.should_descend(&node)? {
        let mut children = ChildMap::new();
        for child in reader.read_children(&node)? {
            let (name, node) = child?;
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

    impl<'a,C,L,T> ReadWalkable<&'a DeepNode<C, L, T>, &'a DeepNode<C, L, T>> for () {
        type Iter = Box<Iterator<Item = Result<(String, &'a DeepNode<C,L,T>)>>+'a>;

        fn read_shallow(&mut self,
                        handle: &'a DeepNode<C, L, T>)
                        -> Result<&'a DeepNode<C, L, T>> {
            Ok(handle)
        }
        fn read_children(&mut self,
                         node: &&'a DeepNode<C, L, T>)
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

    type DummyDeepNode = DeepNode<String, (), ()>;

    #[test]
    fn it_works() {
        let node = DummyDeepNode::tree("Hello".into(), ());
        let read_shallow = ().read_shallow(&node);
        assert_match!(read_shallow, Ok(read_node) if read_node==&node);
    }
}
