import { useEffect, useState } from 'react'
import { api } from '../api'

export default function Migrates() {
  const [parts, setParts] = useState([])
  const [jobs, setJobs] = useState([])
  const [pid, setPid] = useState('')
  const [to, setTo] = useState('冷档')
  const [msg, setMsg] = useState('')
  const reload = () => { api.partitions().then(setParts); api.migrates().then(setJobs) }
  useEffect(() => { reload() }, [])
  async function start() {
    setMsg('')
    try {
      const r = await api.startMigrate({ partition_id: Number(pid), to_tier: to })
      setMsg(r.status === '占坑中'
        ? `迁移单 ${r.job_id} 占坑在途：${r.from_tier}→${r.to_tier}，温层已同单改写`
        : `迁移单 ${r.job_id} 已入队（等候第 ${r.queue_pos} 位，前方占坑单 ${r.hold_job_id}），温层暂不改写`)
      reload()
    } catch (e) { setMsg(e.message) }
  }
  async function finish(id, ok) {
    try {
      const r = await api.finishMigrate(id, ok)
      let m = `迁移单 ${r.job_id} → ${r.status}（温层 ${r.tier}）`
      if (r.promoted) m += `；升占坑单 ${r.promoted.job_id}（${r.promoted.from_tier}→${r.promoted.to_tier}）`
      if (r.rejected && r.rejected.length) m += `；等候单 ${r.rejected.map(x => x.job_id).join('、')} 升占坑被拒`
      setMsg(m)
      reload()
    } catch (e) { setMsg(e.message) }
  }
  const holds = jobs.filter(j => j.status === '占坑中')
  const waits = jobs
    .filter(j => j.status === '等候中')
    .slice()
    .sort((a, b) => a.partition_id - b.partition_id || a.id - b.id)
  return (
    <div className="page">
      <h1>冷热迁移</h1>
      <div className="panel form row">
        <label>分区<select value={pid} onChange={e => setPid(e.target.value)}>
          <option value="">选择</option>
          {parts.filter(p => p.status === '在册').map(p => (
            <option key={p.id} value={p.id}>
              {p.part_key}（{p.tier}）{p.hold_job_id ? `·占坑#${p.hold_job_id}` : ''}{p.wait_count ? `·等候${p.wait_count}` : ''}
            </option>
          ))}
        </select></label>
        <label>目标温层<select value={to} onChange={e => setTo(e.target.value)}>
          <option>热档</option><option>温档</option><option>冷档</option>
        </select></label>
        <button type="button" onClick={start}>发起迁移</button>
      </div>
      <p className="muted">口径：同分区同时至多一笔占坑，占坑即改写温层；再发起整笔入队等候，等候单不改温层；占坑完结按单号升占坑。</p>
      {msg && <p className="msg">{msg}</p>}

      <section className="panel">
        <h2>占坑在途</h2>
        <div className="table-wrap">
          <table>
            <thead><tr><th>单号</th><th>分区</th><th>从</th><th>到</th><th>状态</th><th>操作</th></tr></thead>
            <tbody>
              {holds.map(j => (
                <tr key={j.id}>
                  <td>{j.id}</td><td>{j.part_key}</td><td>{j.from_tier}</td><td>{j.to_tier}</td>
                  <td><span className="badge">{j.status}</span></td>
                  <td>
                    <button type="button" onClick={() => finish(j.id, true)}>完成</button>
                    <button type="button" className="ghost" onClick={() => finish(j.id, false)}>失败回滚</button>
                  </td>
                </tr>
              ))}
              {!holds.length && <tr><td colSpan="6" className="muted">无在途占坑</td></tr>}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <h2>等候队列</h2>
        <div className="table-wrap">
          <table>
            <thead><tr><th>队次</th><th>单号</th><th>分区</th><th>自记从</th><th>到</th><th>状态</th></tr></thead>
            <tbody>
              {waits.map(j => (
                <tr key={j.id}>
                  <td>第 {j.queue_pos} 位</td><td>{j.id}</td><td>{j.part_key}</td><td>{j.from_tier}</td><td>{j.to_tier}</td>
                  <td><span className="badge wait">{j.status}</span></td>
                </tr>
              ))}
              {!waits.length && <tr><td colSpan="6" className="muted">无等候单</td></tr>}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <h2>迁移流水</h2>
        <div className="table-wrap">
          <table>
            <thead><tr><th>单号</th><th>分区</th><th>从</th><th>到</th><th>状态</th><th>队次</th><th>备注</th></tr></thead>
            <tbody>
              {jobs.map(j => (
                <tr key={j.id}>
                  <td>{j.id}</td><td>{j.part_key}</td><td>{j.from_tier}</td><td>{j.to_tier}</td>
                  <td>{j.status}</td>
                  <td>{j.status === '等候中' ? `第 ${j.queue_pos} 位` : '—'}</td>
                  <td className="muted">{j.note || '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  )
}
