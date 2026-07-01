import { useEffect } from 'react'
import 'leaflet/dist/leaflet.css'
import { CircleMarker, MapContainer, TileLayer, Tooltip, useMap } from 'react-leaflet'
import type { LatLngBoundsExpression } from 'leaflet'

// Kept in sync with CHART_COLORS in components.tsx so a marker's colour matches
// its legend swatch (this file is lazy-loaded, so it can't import from there
// without pulling the whole renderer into the map chunk).
const MARKER_COLORS = ['#2563eb', '#10b981', '#f59e0b', '#ef4444', '#06b6d4', '#a855f7']

// Public OpenStreetMap tile server — the map's data source (not an internal
// endpoint). Override via the VITE_MAP_TILE_URL env var for a different provider.
const TILE_URL =
  (import.meta as unknown as { env?: Record<string, string> }).env?.VITE_MAP_TILE_URL ||
  'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png'
const TILE_ATTRIBUTION = '&copy; OpenStreetMap contributors'

export interface MapPoint {
  lat: number
  lng: number
  label: string
  value: number
}

// Re-fit the viewport to the data whenever the points change (covers the case
// where MapContainer's initial `bounds` is set before layout settles).
function FitBounds({ bounds }: { bounds: LatLngBoundsExpression }) {
  const map = useMap()
  useEffect(() => {
    try {
      map.fitBounds(bounds, { padding: [40, 40], maxZoom: 15 })
    } catch {
      /* degenerate bounds (single point) — leaflet keeps the default view */
    }
  }, [map, bounds])
  return null
}

export default function LeafletMap({
  points,
  hasValues,
  maxVal,
}: {
  points: MapPoint[]
  hasValues: boolean
  maxVal: number
}) {
  const lats = points.map((p) => p.lat)
  const lngs = points.map((p) => p.lng)
  const bounds: LatLngBoundsExpression = [
    [Math.min(...lats), Math.min(...lngs)],
    [Math.max(...lats), Math.max(...lngs)],
  ]
  const radius = (v: number) => (hasValues && Number.isFinite(v) && v > 0 ? 8 + (v / maxVal) * 14 : 9)

  return (
    <MapContainer bounds={bounds} scrollWheelZoom style={{ height: 420, width: '100%' }}>
      <TileLayer url={TILE_URL} attribution={TILE_ATTRIBUTION} />
      <FitBounds bounds={bounds} />
      {points.map((p, i) => {
        const color = MARKER_COLORS[i % MARKER_COLORS.length]
        return (
          <CircleMarker
            key={i}
            center={[p.lat, p.lng]}
            radius={radius(p.value)}
            pathOptions={{ color, fillColor: color, fillOpacity: 0.6, weight: 2 }}
          >
            <Tooltip direction="top" offset={[0, -4]}>
              <span style={{ fontWeight: 600 }}>{p.label || `${p.lat.toFixed(3)}, ${p.lng.toFixed(3)}`}</span>
              {hasValues && Number.isFinite(p.value) && p.value > 0 ? ` · ${p.value}` : ''}
            </Tooltip>
          </CircleMarker>
        )
      })}
    </MapContainer>
  )
}
