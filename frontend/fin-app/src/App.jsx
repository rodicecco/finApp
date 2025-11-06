import './App.css'
import 'bootstrap/dist/css/bootstrap.min.css';
import EconLineChart from './components/EconLineChart'

function App() {

  return (
    <>
      <nav className="navbar bg-body-secondary">
        <div className="container-fluid">
          <h6>DAILY FIGURES & INFOMRATION</h6>
        </div>
      </nav>
      <div className="container mt-4">
        <div className="row bg-tertiary chart-row" style={{ minHeight: '420px' }}>
          <div className="col-6 chart-placeholder" >
            <EconLineChart seriesIds={['GDP','UNRATE']} />
          </div>
          <div className="col-6 chart-placeholder">
            <EconLineChart seriesIds={['CPIAUCSL','GS10']} useYoY={true} />
          </div>
        </div>
      </div>
    </>
  )
}

export default App
