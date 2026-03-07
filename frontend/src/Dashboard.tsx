import { useEffect, useState } from "react"
import { Bar } from "react-chartjs-2"
import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    BarElement,
    Title,
    Tooltip,
} from "chart.js"

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip)

interface ScoreBucket {
    bucket: string
    count: number
}

export default function Dashboard() {
    const [scores, setScores] = useState<ScoreBucket[]>([])
    const [loading, setLoading] = useState(true)

    useEffect(() => {
        const token = localStorage.getItem("api_key")

        fetch("/analytics/analytics/scores?lab=lab-05", {
            headers: {
                Authorization: `Bearer ${token}`,
            },
        })
            .then((r) => r.json())
            .then((data) => {
                if (Array.isArray(data)) setScores(data)
                setLoading(false)
            })
            .catch((e) => {
                console.error(e)
                setLoading(false)
            })
    }, [])

    if (loading) {
        return <p>Loading chart...</p>
    }

    const data = {
        labels: scores.map((s) => s.bucket),
        datasets: [
            {
                label: "Score Distribution",
                data: scores.map((s) => s.count),
            },
        ],
    }

    return (
        <div>
            <h2>Score Distribution</h2>
            <Bar data={data} />
        </div>
    )
}