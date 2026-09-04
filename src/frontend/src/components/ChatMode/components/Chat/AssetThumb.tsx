import React from 'react';
import { useAssetDataUrl } from '../../hooks/useResolvedAssetHtml';

/** A small preview of an attached image, fetched through the API (auth) and
 *  shown once its bytes are in; a quiet placeholder before that. */
const AssetThumb: React.FC<{ id: string; name?: string; size?: number; className?: string }> = ({
  id,
  name,
  size = 40,
  className = '',
}) => {
  const url = useAssetDataUrl(id);
  return (
    <span
      className={`inline-block overflow-hidden rounded-md flex-shrink-0 ${className}`}
      style={{ width: size, height: size, background: 'var(--bg-secondary)' }}
      aria-label={name ? `Image ${name}` : 'Attached image'}
      role="img"
    >
      {url && <img src={url} alt={name || ''} style={{ width: '100%', height: '100%', objectFit: 'cover' }} />}
    </span>
  );
};

export default AssetThumb;
