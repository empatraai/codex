use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WaitNodeKind {
    Task,
    Join,
    Message,
    Approval,
    Resource,
    WorkspaceLock,
    Human,
    External,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WaitOwnership {
    Scheduler,
    External,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct WaitNode {
    pub kind: WaitNodeKind,
    pub id: String,
    pub ownership: WaitOwnership,
}

impl WaitNode {
    pub fn scheduler(kind: WaitNodeKind, id: impl Into<String>) -> Self {
        Self {
            kind,
            id: id.into(),
            ownership: WaitOwnership::Scheduler,
        }
    }

    pub fn external(kind: WaitNodeKind, id: impl Into<String>) -> Self {
        Self {
            kind,
            id: id.into(),
            ownership: WaitOwnership::External,
        }
    }

    pub const fn is_scheduler_owned(&self) -> bool {
        matches!(self.ownership, WaitOwnership::Scheduler)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitGraphError {
    EmptyNodeId {
        kind: WaitNodeKind,
    },
    InvalidOwnership {
        kind: WaitNodeKind,
        ownership: WaitOwnership,
    },
    CycleDetected {
        path: Vec<WaitNode>,
    },
}

impl fmt::Display for WaitGraphError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyNodeId { kind } => write!(formatter, "empty id for {kind:?} wait node"),
            Self::InvalidOwnership { kind, ownership } => {
                write!(
                    formatter,
                    "invalid {ownership:?} ownership for {kind:?} wait node"
                )
            }
            Self::CycleDetected { path } => {
                write!(formatter, "scheduler wait cycle detected")?;
                for node in path {
                    write!(formatter, " -> {:?}:{}", node.kind, node.id)?;
                }
                Ok(())
            }
        }
    }
}

impl Error for WaitGraphError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitEdgeOutcome {
    Added,
    Unchanged,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WaitGraph {
    edges: BTreeMap<WaitNode, BTreeSet<WaitNode>>,
}

impl WaitGraph {
    pub fn add_wait(
        &mut self,
        waiter: WaitNode,
        dependency: WaitNode,
    ) -> Result<WaitEdgeOutcome, WaitGraphError> {
        validate_node(&waiter)?;
        validate_node(&dependency)?;

        if self.has_wait(&waiter, &dependency) {
            return Ok(WaitEdgeOutcome::Unchanged);
        }

        if waiter.is_scheduler_owned()
            && dependency.is_scheduler_owned()
            && let Some(mut path) = self.scheduler_path(&dependency, &waiter)
        {
            path.insert(0, waiter);
            return Err(WaitGraphError::CycleDetected { path });
        }

        self.edges.entry(waiter).or_default().insert(dependency);
        Ok(WaitEdgeOutcome::Added)
    }

    pub fn has_wait(&self, waiter: &WaitNode, dependency: &WaitNode) -> bool {
        self.edges
            .get(waiter)
            .is_some_and(|dependencies| dependencies.contains(dependency))
    }

    pub fn dependencies<'a>(&'a self, waiter: &WaitNode) -> impl Iterator<Item = &'a WaitNode> {
        self.edges.get(waiter).into_iter().flatten()
    }

    pub fn remove_wait(&mut self, waiter: &WaitNode, dependency: &WaitNode) -> bool {
        let removed = match self.edges.get_mut(waiter) {
            Some(dependencies) => dependencies.remove(dependency),
            None => return false,
        };
        if removed && self.edges.get(waiter).is_some_and(BTreeSet::is_empty) {
            self.edges.remove(waiter);
        }
        removed
    }

    pub fn remove_node(&mut self, node: &WaitNode) -> usize {
        let mut removed = self.edges.remove(node).map_or(0, |edges| edges.len());
        self.edges.retain(|_, dependencies| {
            removed += usize::from(dependencies.remove(node));
            !dependencies.is_empty()
        });
        removed
    }

    fn scheduler_path(&self, start: &WaitNode, goal: &WaitNode) -> Option<Vec<WaitNode>> {
        if start == goal {
            return Some(vec![start.clone()]);
        }

        let mut queue = VecDeque::from([(start.clone(), vec![start.clone()])]);
        let mut visited = BTreeSet::from([start.clone()]);
        while let Some((node, path)) = queue.pop_front() {
            let Some(dependencies) = self.edges.get(&node) else {
                continue;
            };
            for dependency in dependencies {
                if !dependency.is_scheduler_owned() || !visited.insert(dependency.clone()) {
                    continue;
                }
                let mut next_path = path.clone();
                next_path.push(dependency.clone());
                if dependency == goal {
                    return Some(next_path);
                }
                queue.push_back((dependency.clone(), next_path));
            }
        }
        None
    }
}

fn validate_node(node: &WaitNode) -> Result<(), WaitGraphError> {
    if node.id.trim().is_empty() {
        return Err(WaitGraphError::EmptyNodeId { kind: node.kind });
    }
    if node.is_scheduler_owned()
        && matches!(node.kind, WaitNodeKind::Human | WaitNodeKind::External)
    {
        return Err(WaitGraphError::InvalidOwnership {
            kind: node.kind,
            ownership: node.ownership,
        });
    }
    Ok(())
}
