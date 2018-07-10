package com.skk.training.visualization;

import java.awt.Color;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

public class VisualizeData extends ApplicationFrame {

	private static final long serialVersionUID = 1L;
	private static Configuration conf = null;

	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}

	public VisualizeData(final String title) throws Exception {

		super(title);

		final XYDataset dataset = fetchHBaseData();
		final JFreeChart chart = createChart(dataset);
		final ChartPanel chartPanel = new ChartPanel(chart);
		chartPanel.setPreferredSize(new java.awt.Dimension(500, 270));
		setContentPane(chartPanel);

	}

	@SuppressWarnings({ "resource", "deprecation" })
	private XYDataset fetchHBaseData() throws Exception {

		final XYSeries series1 = new XYSeries("SKU sales data");
		try {
			HTable table = new HTable(conf, "salesdata");
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);

			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					String skustore = new String(kv.getRow());
					series1.add(Double.parseDouble(skustore.split("-")[3]),
							Double.parseDouble(new String(kv.getValue())));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		final XYSeriesCollection dataset = new XYSeriesCollection();
		dataset.addSeries(series1);

		return dataset;
	}

	private JFreeChart createChart(final XYDataset dataset) {

		final JFreeChart chart = ChartFactory.createXYLineChart("SKU Wise Sales Report", "SKUs", // x axis label
				"$", // y axis label
				dataset, // data
				PlotOrientation.VERTICAL, true, // include legend
				true, // tooltips
				false // urls
		);

		// NOW DO SOME OPTIONAL CUSTOMISATION OF THE CHART...
		chart.setBackgroundPaint(Color.white);

		final XYPlot plot = chart.getXYPlot();
		plot.setBackgroundPaint(Color.LIGHT_GRAY);

		plot.setDomainGridlinePaint(Color.white);
		plot.setRangeGridlinePaint(Color.WHITE);

		final XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
		renderer.setSeriesPaint(0, Color.BLACK);
		renderer.setSeriesLinesVisible(0, true);
		plot.setRenderer(renderer);

		final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
		rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());

		return chart;

	}

	public static void main(final String[] args) throws Exception {

		for (int i = 0; i < 100; i++) {
			final VisualizeData demo = new VisualizeData("SKU Wise Sales Report");

			System.out.println("Re-creating");
			demo.repaint();
			demo.pack();

			RefineryUtilities.centerFrameOnScreen(demo);
			demo.setVisible(true);
			Thread.sleep(30000);
			demo.hide();
		}

	}

}
