package com.linkedin.datastream.common.diag;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * This object holds a mapping between a physical source (by its name) to its position.
 * @see PhysicalSourcePosition
 */
public class PhysicalSources {

  /**
   * A mapping from a physical source's name to its position.
   */
  private final Map<String, PhysicalSourcePosition> _physicalSourceToPosition = new ConcurrentHashMap<>();

  /**
   * Default constructor.
   */
  public PhysicalSources() {
  }

  /**
   * Updates a physical source's existing position using the freshest data available between the existing position data
   * and the new position data.
   *
   * @param physicalSource the physical source to update
   * @param position the new position data
   *
   * @see PhysicalSourcePosition#merge(PhysicalSourcePosition, PhysicalSourcePosition)
   */
  public void update(String physicalSource, PhysicalSourcePosition position) {
    PhysicalSourcePosition existingPosition = _physicalSourceToPosition.get(physicalSource);
    PhysicalSourcePosition mergedPosition = PhysicalSourcePosition.merge(existingPosition, position);
    if (mergedPosition != null) {
      _physicalSourceToPosition.put(physicalSource, mergedPosition);
    }
  }

  /**
   * Retains only the physical sources in the specified collection (removes all others). In other words, removes from
   * the mappings of physical source -> position, all entries where the physical source is not contained in the
   * specified collection.
   * @param physicalSources the list of physical sources to keep
   */
  public void retainAll(Collection<String> physicalSources) {
    _physicalSourceToPosition.keySet().retainAll(physicalSources);
  }

  /**
   * Gets the physical source's existing position.
   * @param physicalSource the physical source name
   * @return the physical source's position data, or null if it doesn't exist
   */
  public PhysicalSourcePosition get(String physicalSource) {
    return _physicalSourceToPosition.get(physicalSource);
  }

  /**
   * Getter returns the current physical source to position map.
   *
   * This method is used for JSON serialization.
   *
   * @return an immutable copy of the current physical source to position map
   */
  public Map<String, PhysicalSourcePosition> getPhysicalSourceToPosition() {
    return _physicalSourceToPosition;
  }

  /**
   * Setter causes the entry set of the current physical source to position map to match the provided one. If a null
   * map is provided, the current map will be cleared.
   *
   * This method is used for JSON serialization.
   *
   * @param physicalSourceToPosition a map to set the physical sources to position map to
   */
  public void setPhysicalSourceToPosition(Map<String, PhysicalSourcePosition> physicalSourceToPosition) {
    _physicalSourceToPosition.clear();
    if (physicalSourceToPosition != null) {
      _physicalSourceToPosition.putAll(physicalSourceToPosition);
    }
  }

  /**
   * A simple String representation of this object suitable for use in debugging or logging.
   * @return a simple String representation of this object
   */
  @Override
  public String toString() {
    return "PhysicalSources{" + "_physicalSourceToPosition=" + _physicalSourceToPosition + '}';
  }
}