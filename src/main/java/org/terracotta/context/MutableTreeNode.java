/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package org.terracotta.context;

import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

class MutableTreeNode<I, K, V> extends AbstractTreeNode<I, K, V> {

  private final CopyOnWriteArraySet<AbstractTreeNode<I, K, V>> parents = new CopyOnWriteArraySet<AbstractTreeNode<I, K, V>>();

  private final ContextElement<I, K, V> context;

  public MutableTreeNode(ContextElement<I, K, V> context) {
    this.context = context;
  }

  @Override
  public ContextElement<I, K, V> getContext() {
    return context;
  }
  
  @Override
  public String toString() {
    return "{" + context + "}";
  }

  @Override
  Set<AbstractTreeNode<I, K, V>> getAncestors() {
    Set<AbstractTreeNode<I, K, V>> ancestors = Collections.newSetFromMap(new IdentityHashMap<AbstractTreeNode<I, K, V>, Boolean>());
    ancestors.addAll(parents);
    for (AbstractTreeNode<I, K, V> parent : parents) {
      ancestors.addAll(parent.getAncestors());
    }
    return Collections.unmodifiableSet(ancestors);
  }

  @Override
  public Collection<ContextListener> getListeners() {
    return Collections.emptyList();
  }

  @Override
  void addedParent(AbstractTreeNode<I, K, V> parent) {
    parents.add(parent);
  }

  @Override
  void removedParent(AbstractTreeNode<I, K, V> parent) {
    parents.remove(parent);
  }
}
