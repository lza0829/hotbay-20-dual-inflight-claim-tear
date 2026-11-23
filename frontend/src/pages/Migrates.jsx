import { Fragment, useEffect, useState } from 'react'
import { api } from '../api'

const TIERS = ['热档', '温档', '冷档']

export default function Migrates() {
  const [parts, setParts] = useState([])
  const [jobs, setJobs] = useState([])
  const [locks, setLocks] = useState([])
  const [waiting, setWaiting] = useState([])
  const [probeRuns, setProbeRuns] = useState([])
  const [pid, setPid] = useState('')
  const [to, setTo] = useState('冷档')
  const [workers, setWorkers] = useState(4)
  const [msg, setMsg] = useState('')
  const [detail, setDetail] = useState(null)
  const [openRun, setOpenRun] = useState(null)

  const reload = () => {
    api.partitions().then(setParts)
    api.migrates().then(setJobs)
    api.migrateLocks().then(setLocks)
    api.migrateWaiting().then(setWaiting)
    api.probeRuns().then(setProbeRuns)
  }
  useEffect(() => { reload() }, [])

  async function start() {
    setMsg('')
    try {
      const r = await api.startMigrate({ partition_id: Number(pid), to_tier: to })
      setMsg(r.status === '占坑中'
        ? `迁移单 ${r.job_id} 占坑在途：${r.from_tier}→${r.to_tier}，温层已同单锁死`
        : `迁移单 ${r.job_id} 已入队（等候第 ${r.queue_pos} 位，前方占坑单 ${r.hold_job_id}），温层暂不改写`)
      reload()
    } catch (e) { setMsg(e.message) }
  }

  async function finish(id, ok) {
    setMsg('')
    try {
      const r = await api.finishMigrate(id, ok)
      let m = `迁移单 ${r.job_id} → ${r.status}（温层 ${r.tier}）`
      if (r.promoted) m += `；升占坑单 ${r.promoted.job_id}（${r.promoted.from_tier}→${r.promoted.to_tier}）`
      if (r.rejected && r.rejected.length) m += `；等候单 ${r.rejected.map(x => x.job_id).join('、')} 升占坑被拒`
      setMsg(m)
      reload()
    } catch (e) { setMsg(e.message) }
  }

  async function runProbe() {
    setMsg('')
    try {
      const r = await api.runProbe({ partition_id: Number(pid), workers: Number(workers) })
      setMsg(`探针 ${r.run_id} 判定【${r.status}】：${r.summary}`)
      reload()
      setOpenRun(r.run_id)
    } catch (e) { setMsg(e.message) }
  }

  async function openDetail(id) {
    try { setDetail(await api.migrateDetail(id)) } catch (e) { setMsg(e.message) }
  }

  const waits = waiting
    .slice()
    .sort((a, b) => a.partition_id - b.partition_id || a.id - b.id)
  const torn = locks.some(l => l.tier_frozen !== 1)

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
          {TIERS.map(t => <option key={t}>{t}</option>)}
        </select></label>
        <button type="button" onClick={start} disabled={!pid}>发起迁移</button>
        <label>探针并发度<select value={workers} onChange={e => setWorkers(e.target.value)}>
          {[2, 3, 4, 5, 6, 7, 8].map(n => <option key={n} value={n}>{n}</option>)}
        </select></label>
        <button type="button" className="ghost" onClick={runProbe} disabled={!pid}>跑并发探针</button>
      </div>
      <p className="muted">口径：同分区同时至多一笔占坑，占坑即同事务锁死温层；再发起整笔入队等候，等候单不碰温层；占坑完结按单号升占坑，同层等候单整笔拒绝记拒因。总览占坑徽章只由锁行∪温层锁死标记重放。</p>
      {msg && <p className="msg">{msg}</p>}
      {torn && <p className="err">锁行温层未锁死：存在锁行与分区温层分叉，禁止视为正常占坑</p>}

      <section className="panel">
        <h2>占坑在途（锁行读模型）</h2>
        <div className="table-wrap">
          <table>
            <thead><tr><th>单号</th><th>分区</th><th>从</th><th>到</th><th>现行温层</th><th>温层锁死</th><th>操作</th></tr></thead>
            <tbody>
              {locks.map(j => (
                <tr key={j.job_id}>
                  <td>{j.job_id}</td><td>{j.part_key}</td><td>{j.from_tier}</td><td>{j.to_tier}</td>
                  <td>{j.current_tier}</td>
                  <td><span className={`badge ${j.tier_frozen ? '' : 'wait'}`}>{j.tier_frozen ? '已锁死' : '分叉'}</span></td>
                  <td>
                    <button type="button" onClick={() => finish(j.job_id, true)}>完成</button>
                    <button type="button" className="ghost" onClick={() => finish(j.job_id, false)}>失败回滚</button>
                  </td>
                </tr>
              ))}
              {!locks.length && <tr><td colSpan="7" className="muted">无在途占坑</td></tr>}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <h2>等候队列（等候读模型）</h2>
        <div className="table-wrap">
          <table>
            <thead><tr><th>队次</th><th>单号</th><th>分区</th><th>自记从</th><th>到</th><th>前方占坑单</th><th>状态</th></tr></thead>
            <tbody>
              {waits.map(j => (
                <tr key={j.job_id}>
                  <td>第 {j.queue_pos} 位</td><td>{j.job_id}</td><td>{j.part_key}</td>
                  <td>{j.from_tier}</td><td>{j.to_tier}</td>
                  <td>#{j.hold_job_id ?? '—'}</td>
                  <td><span className="badge wait">等候中</span></td>
                </tr>
              ))}
              {!waits.length && <tr><td colSpan="7" className="muted">无等候单</td></tr>}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <h2>迁移流水（含拒因，点单号看详情）</h2>
        <div className="table-wrap">
          <table>
            <thead><tr><th>单号</th><th>分区</th><th>从</th><th>到</th><th>状态</th><th>队次</th><th>备注/拒因</th></tr></thead>
            <tbody>
              {jobs.map(j => (
                <tr key={j.id} className={j.note ? 'row-link' : ''} onClick={() => openDetail(j.id)}>
                  <td>{j.id}</td><td>{j.part_key}</td><td>{j.from_tier}</td><td>{j.to_tier}</td>
                  <td><span className={`badge ${j.status === '等候中' ? 'wait' : ''} ${j.status === '已拒绝' || j.status === '已失败' ? 'danger-badge' : ''}`}>{j.status}</span></td>
                  <td>{j.status === '等候中' ? `第 ${j.queue_pos} 位` : '—'}</td>
                  <td className="muted">{j.note || '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <h2>并发探针流水</h2>
        <div className="table-wrap">
          <table>
            <thead><tr><th>探针号</th><th>分区</th><th>判定</th><th>摘要</th><th>时刻</th><th>事件</th></tr></thead>
            <tbody>
              {probeRuns.map(r => (
                <Fragment key={r.id}>
                  <tr className="row-link" onClick={() => setOpenRun(openRun === r.id ? null : r.id)}>
                    <td>{r.id}</td><td>{r.partition_id}</td>
                    <td><span className={`badge ${r.status === '闭合' ? '' : 'danger-badge'}`}>{r.status}</span></td>
                    <td className="muted">{r.summary}</td><td>{r.created_at}</td>
                    <td>{r.events.length} 条</td>
                  </tr>
                  {openRun === r.id && (
                    <tr>
                      <td colSpan="6">
                        <table className="sub-table">
                          <thead><tr><th>序</th><th>线程</th><th>事件</th><th>单号</th><th>证据</th><th>时刻</th></tr></thead>
                          <tbody>
                            {r.events.map(e => (
                              <tr key={e.seq}>
                                <td>{e.seq}</td><td>{e.thread_label || '—'}</td><td>{e.event}</td>
                                <td>{e.job_id ?? '—'}</td><td className="muted">{e.detail}</td><td>{e.at}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </td>
                    </tr>
                  )}
                </Fragment>
              ))}
              {!probeRuns.length && <tr><td colSpan="6" className="muted">暂无探针流水</td></tr>}
            </tbody>
          </table>
        </div>
      </section>

      {detail && (
        <div className="modal-mask" onClick={() => setDetail(null)}>
          <div className="modal" onClick={e => e.stopPropagation()}>
            <h2>迁移单 {detail.id} 详情</h2>
            <table className="sub-table">
              <tbody>
                <tr><td>分区</td><td>{detail.part_key}（{detail.title}）</td></tr>
                <tr><td>状态</td><td>{detail.status}</td></tr>
                <tr><td>从 / 到</td><td>{detail.from_tier} → {detail.to_tier}</td></tr>
                <tr><td>现行温层</td><td>{detail.current_tier}</td></tr>
                <tr><td>温层锁死</td><td>{detail.tier_frozen ? '已锁死（锁行目标==现行温层）' : '未锁死/非占坑单'}</td></tr>
                {detail.status === '等候中' && (
                  <>
                    <tr><td>队次</td><td>第 {detail.queue_pos} 位</td></tr>
                    <tr><td>前方占坑</td><td>{detail.hold ? `#${detail.hold.hold_job_id} → ${detail.hold.hold_to_tier}` : '无（异常）'}</td></tr>
                  </>
                )}
                <tr><td>发起时刻</td><td>{detail.created_at}</td></tr>
                {detail.finished_at && <tr><td>完结时刻</td><td>{detail.finished_at}</td></tr>}
                <tr><td>备注/拒因</td><td>{detail.note || '—'}</td></tr>
              </tbody>
            </table>
            <button type="button" onClick={() => setDetail(null)}>关闭</button>
          </div>
        </div>
      )}
    </div>
  )
}
