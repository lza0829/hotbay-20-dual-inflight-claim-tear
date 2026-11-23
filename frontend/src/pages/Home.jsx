import { useEffect, useState } from 'react'
import { api } from '../api'

export default function Home() {
  const [o, setO] = useState(null)
  const [err, setErr] = useState('')
  useEffect(() => { api.overview().then(setO).catch(e => setErr(e.message)) }, [])
  if (err) return <p className="err">{err}</p>
  if (!o) return <p className="muted">加载总览…</p>
  // 占坑徽章只由锁行 ∪ 温层锁死标记重放：锁死数与占坑数恒等，差异即异常
  const claimTorn = o.占坑温层锁死 !== o.迁移占坑中
  const cards = [
    ['热档', o.热档], ['温档', o.温档], ['冷档', o.冷档],
    ['迁移占坑中', o.迁移占坑中], ['占坑温层锁死', o.占坑温层锁死], ['迁移等候中', o.迁移等候中],
    ['待销毁候选', o.待销毁候选], ['目录对象', o.目录对象],
  ]
  return (
    <div className="page">
      <header className="hero">
        <p className="eyebrow">影院素材温层</p>
        <h1>热档片间</h1>
        <p className="lede">分区在册，冷热可迁，片龄到期可销毁，目录与元数据互证。</p>
      </header>
      {claimTorn && (
        <p className="err">占坑证据撕裂：占坑徽章 {o.迁移占坑中} 与温层锁死 {o.占坑温层锁死} 不恒等，请查探针流水</p>
      )}
      <div className="stat-grid">
        {cards.map(([k, v]) => (
          <div className="stat" key={k}><span>{k}</span><strong>{v}</strong></div>
        ))}
      </div>
      <section className="panel">
        <h2>片库间</h2>
        <ul className="plain">
          {o.片库间.map(v => (
            <li key={v.id}>{v.name} · 额定 {v.capacity} · {v.active ? '启用' : '停用'}</li>
          ))}
        </ul>
        {o.最近互证 && (
          <p className="muted">最近互证：{o.最近互证.status} · 漂移 {o.最近互证.drift_count} · {o.最近互证.created_at}</p>
        )}
      </section>
    </div>
  )
}
