package druid

import (
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/pkg/query"
	"strings"
	"time"
)

type Devops struct {
	*BaseGenerator
	*devops.Core
}

func (d *Devops) getTimestamp(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}

func (d *Devops) getSelectClausesAggMetrics(agg string, metrics []string) []string {
	selectClauses := make([]string, len(metrics))
	for i, m := range metrics {
		selectClauses[i] = fmt.Sprintf("%s(%s)", agg, m)
	}

	return selectClauses
}

func (d *Devops) getHostWhereWithHostnames(hostnames []string) string {
	return fmt.Sprintf("hostname IN (%s)", strings.Join(hostnames, ", "))
}

func (d *Devops) getHostWhereString(nHosts int) string {
	hostnames, err := d.GetRandomHosts(nHosts)
	databases.PanicIfErr(err)
	return d.getHostWhereWithHostnames(hostnames)
}

func (d *Devops) HighCPUForHosts(q query.Query, i int) {
	//TODO implement me
	panic("implement me")
}

func (d *Devops) GroupByOrderByLimit(q query.Query) {
	//TODO implement me
	panic("implement me")
}

func (d *Devops) MaxAllCPU(q query.Query, i int, duration time.Duration) {
	//TODO implement me
	panic("implement me")
}

func (d *Devops) LastPointPerHost(q query.Query) {
	//TODO implement me
	panic("implement me")
}

func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	databases.PanicIfErr(err)
	selectClauses := d.getSelectClausesAggMetrics("mean", metrics)

	humanLabel := fmt.Sprintf("Druid %d cpu metric(s)", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	sql := fmt.Sprintf("SELECT FLOOR(\"__time\" to MINUTE) as minutes, hostname, %s from \"cpu-only\" where__time >= TIMESTAMP '%s' and __time < TIMESTAMP '%s' group by 1, hostname", strings.Join(selectClauses, ", "), d.getTimestamp(interval.Start()), d.getTimestamp(interval.End()))
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

func (d *Devops) GroupByTime(qi query.Query, nHosts int, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.StaticWindow(timeRange)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	databases.PanicIfErr(err)
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)
	whereHosts := d.getHostWhereString(nHosts)

	humanLabel := fmt.Sprintf("Druid %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	sql := fmt.Sprintf("SELECT FLOOR(\"__time\" to MINUTE) as minutes, %s from \"cpu-only\" where %s and __time >= TIMESTAMP '%s' and __time < TIMESTAMP '%s' group by 1", strings.Join(selectClauses, ", "), whereHosts, d.getTimestamp(interval.Start()), d.getTimestamp(interval.End()))
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

func (d *Devops) GenerateEmptyQuery() query.Query {
	return query.NewHTTP()
}
