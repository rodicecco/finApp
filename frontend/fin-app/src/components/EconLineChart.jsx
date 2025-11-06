import React, { useEffect, useState } from 'react'
import { Line } from 'react-chartjs-2'
import {
  Chart as ChartJS,
  LineElement,
  PointElement,
  TimeScale,
  LinearScale,
  Title,
  Tooltip,
  Legend
} from 'chart.js'
import 'chartjs-adapter-date-fns'

ChartJS.register(LineElement, PointElement, TimeScale, LinearScale, Title, Tooltip, Legend)

const COLORS = [
  'rgba(54,162,235,0.95)',
  'rgba(255,99,132,0.95)',
  'rgba(75,192,192,0.95)',
  'rgba(255,206,86,0.95)',
  'rgba(153,102,255,0.95)'
]

export default function EconLineChart({ seriesIds = ['GDP'] }){
  const defaultSeries = Array.isArray(seriesIds) && seriesIds.length ? seriesIds[0] : seriesIds

  const [input, setInput] = useState(defaultSeries)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  // store multiple series ids and their fetched data
  const [seriesList, setSeriesList] = useState(Array.isArray(seriesIds) ? seriesIds.slice(0,1) : [defaultSeries])
  const [seriesData, setSeriesData] = useState({})
  const [labelsRaw, setLabelsRaw] = useState([]) // union of dates across added series
  const [startIndex, setStartIndex] = useState(0)

  useEffect(() => {
    // fetch any initial series in seriesList
    seriesList.forEach(id => { if(id) addSeries(id) })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // debug: log seriesList and seriesData changes to help trace disappearing charts
  useEffect(() => {
    try{
      // eslint-disable-next-line no-console
      console.debug('EconLineChart state update - seriesList:', seriesList, 'seriesData keys:', Object.keys(seriesData))
    }catch(e){ }
  }, [seriesList, seriesData])

  // helper to recompute labelsRaw as sorted union of all dates across seriesData
  const rebuildLabelsUnion = (dataMap) => {
    const set = new Set()
    Object.values(dataMap).forEach(d => {
      if(d && d.level) d.level.forEach(pt => set.add(pt.x))
    })
    const arr = Array.from(set).map(x => new Date(x)).sort((a,b)=>a-b).map(d=>d.toISOString())
    setLabelsRaw(arr)
    // ensure startIndex within bounds
    setStartIndex(s => Math.max(0, Math.min(s, Math.max(0, arr.length-1))))
  }

  // fetch and register a series (used for Add)
  async function addSeries(id){
    if(!id) return
    const key = id.trim().toUpperCase()
    if(seriesData[key]) return // already have it
    setLoading(true)
    setError(null)

    const BACKEND = import.meta.env.VITE_BACKEND_URL || 'http://localhost:5000'
    try{
      const resp = await fetch(`${BACKEND}/econdata`, {
        method: 'POST', headers: { 'Content-Type':'application/json' },
        body: JSON.stringify({ series_ids: [key] })
      })
      if(!resp.ok) throw new Error(`Server returned ${resp.status}`)
      const json = await resp.json()
      const payload = json[key]
      if(!payload || !payload.data) throw new Error('No data returned for ' + key)

      const dataDict = payload.data
      const meta = (payload.meta && payload.meta.length) ? payload.meta[0] : null
      const frequency = meta && meta.frequency ? meta.frequency : 'Monthly'

      const labels = dataDict.date || []
      const toPoints = (arr) => labels.map((d,i) => {
        const val = (arr && arr[i] !== undefined) ? arr[i] : null
        const iso = (() => { try { return new Date(d).toISOString() } catch(e){ return d } })()
        return { x: iso, y: val }
      })

      const lvl = toPoints(dataDict[key] || [])
      const periodKey = (frequency && frequency.toLowerCase().startsWith('q')) ? `QoQ_${key}` : `MoM_${key}`
      const period = toPoints(dataDict[periodKey] || [])
      const yoy = toPoints(dataDict[`YoY_${key}`] || [])

      const next = { ...seriesData, [key]: { level: lvl, period, yoy, freq: frequency } }
      setSeriesData(next)
      // ensure id present in list
      setSeriesList(prev => prev.includes(key) ? prev : [...prev, key])
      rebuildLabelsUnion(next)
      setLoading(false)
    }catch(err){
      setError(err.message)
      setLoading(false)
    }
  }

  function removeSeries(id){
    const key = id.trim().toUpperCase()
    if(!seriesData[key]){
      setSeriesList(prev => prev.filter(s=>s!==key))
      return
    }
    const next = { ...seriesData }
    delete next[key]
    setSeriesData(next)
    setSeriesList(prev => prev.filter(s=>s!==key))
    rebuildLabelsUnion(next)
  }

  async function fetchSeries(id){
    setLoading(true)
    setError(null)
    setLevelData(null); setPeriodData(null); setYoyData(null)

    const BACKEND = import.meta.env.VITE_BACKEND_URL || 'http://localhost:5000'
    try{
      const resp = await fetch(`${BACKEND}/econdata`, {
        method: 'POST', headers: { 'Content-Type':'application/json' },
        body: JSON.stringify({ series_ids: [id] })
      })
      if(!resp.ok) throw new Error(`Server returned ${resp.status}`)
      const json = await resp.json()
      const payload = json[id]
      if(!payload || !payload.data) throw new Error('No data returned for ' + id)

      const dataDict = payload.data
      // choose period key based on frequency metadata (Monthly->MoM, Quarterly->QoQ)
      const meta = (payload.meta && payload.meta.length) ? payload.meta[0] : null
      const frequency = meta && meta.frequency ? meta.frequency : 'Monthly'
      setFreq(frequency)

      const labels = dataDict.date || []
      setLabelsRaw(labels)

  // initialize index-based range selector: start at 0, end fixed to last index
  setStartIndex(0)
  setEndIndex(Math.max(0, labels.length - 1))

      const toPoints = (arr) => labels.map((d,i) => {
        const val = (arr && arr[i] !== undefined) ? arr[i] : null
        const iso = (() => { try { return new Date(d).toISOString() } catch(e){ return d } })()
        return { x: iso, y: val }
      })

      // level
      const lvl = toPoints(dataDict[id] || [])
      // period change key
      const periodKey = (frequency && frequency.toLowerCase().startsWith('q')) ? `QoQ_${id}` : `MoM_${id}`
      const period = toPoints(dataDict[periodKey] || [])
      const yoy = toPoints(dataDict[`YoY_${id}`] || [])

      setLevelData(lvl)
      setPeriodData(period)
      setYoyData(yoy)

      setLoading(false)
    }catch(err){
      setError(err.message)
      setLoading(false)
    }
  }

  const formatPercent = (v) => {
    if (v === null || v === undefined || Number.isNaN(v)) return ''
    const num = Number(v) * 100
    // trim insignificant decimals
    const fixed = Math.abs(num) < 10 ? num.toFixed(2) : num.toFixed(1)
    return `${fixed}%`
  }

  const commonOptions = (title, isPercent = false) => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: false },
      title: { display: true, text: title },
      tooltip: {
        callbacks: {
          // show month+year as the tooltip title (the hovered x value)
          title: (items) => {
            if(!items || !items.length) return ''
            const it = items[0]
            // try multiple sources for the x value
            const rawX = it.raw && it.raw.x !== undefined ? it.raw.x : (it.parsed && it.parsed.x !== undefined ? it.parsed.x : it.label)
            try{
              const d = new Date(rawX)
              return d.toLocaleString(undefined, { month: 'long', year: 'numeric' })
            }catch(e){
              return String(rawX)
            }
          },
          label: (context) => {
            const y = context.parsed && context.parsed.y
            if (isPercent) return `${context.dataset.label || ''}: ${formatPercent(y)}`
            return `${context.dataset.label || ''}: ${y === null ? '' : y}`
          }
        }
      }
    },
    scales: {
      x: {
        type: 'time',
        // show years only on the x-axis (e.g. 2020, 2021)
        time: { unit: 'year', tooltipFormat: 'yyyy', displayFormats: { year: 'yyyy' } },
        ticks: { autoSkip: true, maxRotation: 0 }
      },
      y: {
        display: true,
        ticks: isPercent ? {
          callback: (val) => formatPercent(val)
        } : {}
      }
    }
  })

  const buildDataset = (data, label, color, yAxisID) => ({
    label,
    data: data || [],
    borderColor: color,
    backgroundColor: color,
    tension: 0.12,
    pointRadius: 0,
    borderWidth: 1,
    yAxisID
  })

  // slice by selected start date (inclusive). end is always last available point.
  const sliceByStartDate = (arr) => {
    if(!arr || !arr.length) return arr || []
    if(!labelsRaw || !labelsRaw.length) return arr
    const startIso = labelsRaw[Math.max(0, Math.min(startIndex, labelsRaw.length-1))]
    const startDate = new Date(startIso)
    return arr.filter(pt => {
      try{ return new Date(pt.x) >= startDate }catch(e){ return true }
    })
  }

  const isoToInputDate = (iso) => {
    try{
      const d = new Date(iso)
      const yyyy = d.getUTCFullYear()
      const mm = String(d.getUTCMonth() + 1).padStart(2,'0')
      const dd = String(d.getUTCDate()).padStart(2,'0')
      return `${yyyy}-${mm}-${dd}`
    }catch(e){ return '' }
  }

  return (
    <div style={{width:'100%'}}>
      <div style={{display:'flex', gap:8, alignItems:'center', marginBottom:12}}>
        <input value={input} onChange={e => setInput(e.target.value)} placeholder="Enter FRED series id (e.g. GDP)" />
        <button onClick={() => { addSeries(input.trim()) }}>Add</button>
        <button onClick={() => { setSeriesList([]); setSeriesData({}); setLabelsRaw([]); setStartIndex(0) }} style={{marginLeft:8}}>Clear all</button>
      </div>

      {/* active series chips */}
      <div style={{display:'flex', gap:8, flexWrap:'wrap', marginBottom:12}}>
        {seriesList.map((s, idx) => (
          <div key={s} style={{display:'inline-flex', alignItems:'center', gap:8, padding:'4px 8px', borderRadius:6, background:'#f4f4f4'}}>
            <span style={{width:12, height:12, background: COLORS[idx % COLORS.length], display:'inline-block', borderRadius:3}} />
            <strong>{s}</strong>
            <button onClick={() => removeSeries(s)} style={{marginLeft:6}}>x</button>
          </div>
        ))}
      </div>



      {loading && <div>Loading data…</div>}
      {error && <div style={{color:'red'}}>Error: {error}</div>}

      <div style={{marginBottom:12}}>
        {/* Range selector: two draggable range inputs over indices for quick zooming */}
        {labelsRaw && labelsRaw.length > 0 && (
          <div style={{display:'flex', flexDirection:'column', gap:8}}>
              <div style={{display:'flex', gap:12, alignItems:'center'}}>
                <div style={{fontSize:12, color:'#444'}}>From: {isoToInputDate(new Date(labelsRaw[Math.min(startIndex, labelsRaw.length-1)]).toISOString())}</div>
                <div style={{flex:1}}>
                  <input type="range" min={0} max={Math.max(0, labelsRaw.length - 1)} value={startIndex} onChange={e => {
                    const v = Number(e.target.value)
                    setStartIndex(Math.max(0, Math.min(v, Math.max(0, labelsRaw.length - 1))))
                  }} style={{width:'100%', accentColor:'#1e88ff'}} />
                </div>
                <div style={{fontSize:12, color:'#444'}}>To: {labelsRaw && labelsRaw.length ? isoToInputDate(new Date(labelsRaw[labelsRaw.length-1]).toISOString()) : ''}</div>
              </div>

            <div style={{display:'flex', gap:8}}>
              <button onClick={() => { setStartIndex(0); setEndIndex(Math.max(0, labelsRaw.length - 1)) }}>Reset range</button>
            </div>
          </div>
        )}
      </div>

      <div style={{height:220, marginBottom:12}}>
        {/* Level chart: one dataset per series, shared x-axis. Create per-series axes so each series can scale independently. */}
        <LevelChart seriesList={seriesList} seriesData={seriesData} startIndex={startIndex} buildDataset={buildDataset} commonOptions={commonOptions} COLORS={COLORS} sliceFn={sliceByStartDate} />
      </div>

      <div style={{height:180, marginBottom:12}}>
        {/* Period change (MoM/QoQ) chart */}
        <ChangeChart seriesList={seriesList} seriesData={seriesData} startIndex={startIndex} buildDataset={buildDataset} commonOptions={commonOptions} COLORS={COLORS} sliceFn={sliceByStartDate} titleSuffix={'change'} isPercent={true} />
      </div>

      <div style={{height:180}}>
        {/* YoY chart */}
        <ChangeChart seriesList={seriesList} seriesData={seriesData} startIndex={startIndex} buildDataset={buildDataset} commonOptions={commonOptions} COLORS={COLORS} sliceFn={sliceByStartDate} titleSuffix={'YoY'} isPercent={true} />
      </div>
    </div>
  )
}

// --- helper subcomponents built inline to keep file self-contained ---
function LevelChart({ seriesList, seriesData, buildDataset, commonOptions, COLORS, sliceFn }){
  const datasets = seriesList.map((id, idx) => {
    const entry = seriesData[id] || {}
    const data = sliceFn(entry.level || [])
    return buildDataset(data, id, COLORS[idx % COLORS.length], `y-${idx+1}`)
  })

  const options = { ...commonOptions('Level', false), scales: buildScalesForSeries(seriesList, false) }
  return <Line options={options} data={{ datasets }} />
}

function ChangeChart({ seriesList, seriesData, buildDataset, commonOptions, COLORS, sliceFn, titleSuffix, isPercent }){
  const datasets = seriesList.map((id, idx) => {
    const entry = seriesData[id] || {}
    const key = titleSuffix === 'YoY' ? 'yoy' : 'period'
    const data = sliceFn(entry[key] || [])
    return buildDataset(data, id, COLORS[idx % COLORS.length], `y-${idx+1}`)
  })

  const options = { ...commonOptions(titleSuffix, isPercent), scales: buildScalesForSeries(seriesList, isPercent) }
  return <Line options={options} data={{ datasets }} />
}

function buildScalesForSeries(seriesList, isPercent){
  const scales = {
    x: {
      type: 'time',
      time: { unit: 'year', tooltipFormat: 'yyyy', displayFormats: { year: 'yyyy' } },
      ticks: { autoSkip: true, maxRotation: 0 }
    }
  }
  seriesList.forEach((s, idx) => {
    const id = `y-${idx+1}`
    scales[id] = {
      type: 'linear', display: true, position: (idx % 2 === 0) ? 'left' : 'right',
      grid: { drawOnChartArea: idx === 0 },
      ticks: isPercent ? { callback: (v) => {
        if (v === null || v === undefined || Number.isNaN(v)) return ''
        const num = Number(v) * 100
        const fixed = Math.abs(num) < 10 ? num.toFixed(2) : num.toFixed(1)
        return `${fixed}%`
      } } : {}
    }
  })
  return scales
}
