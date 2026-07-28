/**
 * Icon-name -> lucide component lookup used by Card/KeyValue.
 */
import { Lightbulb } from 'lucide-react'
import { AlertTriangle, Award, BarChart3, Brain, Briefcase, Building2, Calendar, CheckCircle2, Clock, Cloud, Cpu, Database, DollarSign, Gauge, Globe, Layers, Link2, Lock, Package, Rocket, Search, Server, Settings, Shield, Star, Target, TrendingDown, TrendingUp, Users, Wrench, Zap } from 'lucide-react'
import type { LucideIcon } from 'lucide-react'
import { asStr } from './values'

// Curated icon allowlist the composer may reference by name (KeyValue/Card
// `icon` prop). Friendly, stable names → lucide components; unknown names render
// nothing (never a broken glyph). Keep in sync with the icon list advertised in
// the composer prompt (compose.py) — the model only knows the names listed there.
const ICON_MAP: Record<string, LucideIcon> = {
  'trending-up': TrendingUp,
  'trending-down': TrendingDown,
  users: Users,
  dollar: DollarSign,
  clock: Clock,
  check: CheckCircle2,
  alert: AlertTriangle,
  target: Target,
  zap: Zap,
  globe: Globe,
  database: Database,
  server: Server,
  shield: Shield,
  rocket: Rocket,
  lightbulb: Lightbulb,
  chart: BarChart3,
  calendar: Calendar,
  settings: Settings,
  search: Search,
  link: Link2,
  cloud: Cloud,
  cpu: Cpu,
  layers: Layers,
  gauge: Gauge,
  award: Award,
  briefcase: Briefcase,
  building: Building2,
  star: Star,
  package: Package,
  wrench: Wrench,
  brain: Brain,
  lock: Lock,
}
export const iconByName = (name: unknown): LucideIcon | null =>
  ICON_MAP[asStr(name).toLowerCase().trim()] ?? null
