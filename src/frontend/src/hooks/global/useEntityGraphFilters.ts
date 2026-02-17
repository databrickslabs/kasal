import { useMemo } from 'react';
import useEntityGraphStore, { EntityNode, EntityLink, GraphData } from '../../store/entityGraphStore';
import { getEntityColor } from '../../utils/entityColors';
import Logger from '../../utils/logger';

const logger = new Logger({ prefix: 'EntityGraphFilters' });

export interface AvailableEntityType {
  type: string;
  color: string;
  count: number;
}

interface UseEntityGraphFiltersResult {
  filteredNodes: EntityNode[];
  filteredLinks: EntityLink[];
  availableEntityTypes: AvailableEntityType[];
}

function getNodeId(nodeOrId: string | EntityNode): string {
  return typeof nodeOrId === 'object' ? nodeOrId.id : nodeOrId;
}

export function useEntityGraphFilters(): UseEntityGraphFiltersResult {
  const graphData = useEntityGraphStore((s) => s.graphData);
  const focusedNodeId = useEntityGraphStore((s) => s.focusedNodeId);
  const deduplicateNodes = useEntityGraphStore((s) => s.deduplicateNodes);
  const showOrphanedNodes = useEntityGraphStore((s) => s.showOrphanedNodes);
  const hiddenEntityTypes = useEntityGraphStore((s) => s.hiddenEntityTypes);

  const availableEntityTypes = useMemo<AvailableEntityType[]>(() => {
    const counts = new Map<string, number>();
    for (const node of graphData.nodes) {
      const t = node.type.toLowerCase();
      counts.set(t, (counts.get(t) || 0) + 1);
    }
    return Array.from(counts.entries())
      .sort((a, b) => b[1] - a[1])
      .map(([type, count]) => ({
        type,
        color: getEntityColor(type),
        count,
      }));
  }, [graphData.nodes]);

  const { filteredNodes, filteredLinks } = useMemo(() => {
    let nodes = [...graphData.nodes];
    let links = [...graphData.links];

    // Filter by hidden entity types
    if (hiddenEntityTypes.size > 0) {
      nodes = nodes.filter((n) => !hiddenEntityTypes.has(n.type.toLowerCase()));
      const nodeIds = new Set(nodes.map((n) => n.id));
      links = links.filter((l) => nodeIds.has(getNodeId(l.source)) && nodeIds.has(getNodeId(l.target)));
    }

    // Compute connected node IDs
    const connectedNodeIds = new Set<string>();
    links.forEach((link) => {
      connectedNodeIds.add(getNodeId(link.source));
      connectedNodeIds.add(getNodeId(link.target));
    });

    // Remove orphaned nodes unless showing them or in focused mode
    if (!focusedNodeId && !showOrphanedNodes && connectedNodeIds.size > 0) {
      const orphanedCount = nodes.filter((node) => !connectedNodeIds.has(node.id)).length;
      nodes = nodes.filter((node) => connectedNodeIds.has(node.id));
      if (orphanedCount > 0) {
        logger.debug(`Filtered out ${orphanedCount} orphaned nodes`);
      }
    }

    // Focus filter
    if (focusedNodeId) {
      const focusedNode = nodes.find((n) => n.id === focusedNodeId);
      if (focusedNode) {
        const focusConnected = new Set([focusedNodeId]);
        links.forEach((link) => {
          const sourceId = getNodeId(link.source);
          const targetId = getNodeId(link.target);
          if (sourceId === focusedNodeId) focusConnected.add(targetId);
          if (targetId === focusedNodeId) focusConnected.add(sourceId);
        });
        nodes = nodes.filter((n) => focusConnected.has(n.id));
        links = links.filter((l) => focusConnected.has(getNodeId(l.source)) && focusConnected.has(getNodeId(l.target)));
      }
    } else if (deduplicateNodes) {
      // Deduplication
      logger.debug('Starting deduplication. Original nodes:', nodes.length, 'Original links:', links.length);
      const idMapping = new Map<string, string>();
      const uniqueNodes = new Map<string, EntityNode>();

      nodes.forEach((node) => {
        if (!uniqueNodes.has(node.name)) {
          uniqueNodes.set(node.name, node);
          idMapping.set(node.id, node.id);
        } else {
          const canonical = uniqueNodes.get(node.name)!;
          idMapping.set(node.id, canonical.id);
        }
      });

      nodes = Array.from(uniqueNodes.values());
      const nodeIds = new Set(nodes.map((n) => n.id));

      links = links
        .map((link) => {
          const originalSource = getNodeId(link.source);
          const originalTarget = getNodeId(link.target);
          return {
            ...link,
            source: idMapping.get(originalSource) || originalSource,
            target: idMapping.get(originalTarget) || originalTarget,
          };
        })
        .filter((link) => {
          const s = getNodeId(link.source);
          const t = getNodeId(link.target);
          return s !== t && nodeIds.has(s) && nodeIds.has(t);
        });

      // Remove duplicate links
      const uniqueLinks = new Map<string, EntityLink>();
      links.forEach((link) => {
        const key = `${getNodeId(link.source)}-${getNodeId(link.target)}-${link.relationship || 'related'}`;
        if (!uniqueLinks.has(key)) {
          uniqueLinks.set(key, link);
        }
      });
      links = Array.from(uniqueLinks.values());

      logger.debug('Final deduplicated state - Nodes:', nodes.length, 'Links:', links.length);
    }

    return { filteredNodes: nodes, filteredLinks: links } as { filteredNodes: EntityNode[]; filteredLinks: EntityLink[] };
  }, [graphData, focusedNodeId, deduplicateNodes, showOrphanedNodes, hiddenEntityTypes]);

  return { filteredNodes, filteredLinks, availableEntityTypes };
}

export type { GraphData };
