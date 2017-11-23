/**********************************************************************
 **                                                                   **
 **               This code belongs to the KETTLE project.            **
 **                                                                   **
 ** Kettle, from version 2.2 on, is released into the public domain   **
 ** under the Lesser GNU Public License (LGPL).                       **
 **                                                                   **
 ** For more details, please read the document LICENSE.txt, included  **
 ** in this project                                                   **
 **                                                                   **
 ** http://www.kettle.be                                              **
 ** info@kettle.be                                                    **
 **                                                                   **
 **********************************************************************/

package org.pentaho.di.trans.steps.solrin;

import org.pentaho.di.compatibility.Value;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

/**
 * Retrieves values from a database by calling database stored procedures or
 * functions
 *
 * @author Ian Holsman (based on Matt's HTTP)
 * @since 8-July-2007
 */

public class SolrIn extends BaseStep implements StepInterface {
	private SolrInMeta meta;
	private SolrInData data;
	protected String url;
	protected HttpSolrClient server;
	protected int rowCount;

	public static final String POST_ENCODING = "UTF-8";

	public SolrIn(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
			Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	private void execHttp(Object[] r) throws KettleException {
		if (first) {
			first = false;
			data.argnrs = new int[meta.getArgumentField().length];

			for (int i = 0; i < meta.getArgumentField().length; i++) {
				data.argnrs[i] = getInputRowMeta().indexOfValue(meta.getArgumentField()[i]);
				if (data.argnrs[i] < 0) {
					logError(Messages.getString("SolrIn.Log.ErrorFindingField") + meta.getArgumentField()[i] + "]"); //$NON-NLS-1$ //$NON-NLS-2$
					throw new KettleStepException(
							Messages.getString("SolrIn.Exception.CouldnotFindField", meta.getArgumentField()[i])); //$NON-NLS-1$ //$NON-NLS-2$
				}
			}
		}

		// Value result = callHttpService(r);
		// r.addValue(result);

		callHttpService(r);
	}

	private Value callHttpService(Object[] r) throws KettleException {

		UpdateResponse upres;

		try {

			SolrInputDocument doc = new SolrInputDocument();
			logDetailed("Connecting to : [" + url + "]");

			for (int i = 0; i < data.argnrs.length; i++) {
				String key = meta.getArgumentParameter()[i];
				Object value = r[data.argnrs[i]];

				// logDetailed("key:" + key + "\tvalue:"+value);
				if (value != null) // 过滤null记录
				{
					if ("id".equals(key)) {// 如果为主键
						doc.addField(key, value);
					} else {// 非主键单值类型，采用局部set更新
						Map<String, Object> map = new HashMap<String, Object>();
						map.put("set", value);
						doc.addField(key, map);
					} // 对应多值类型，后期扩展
				}
			}
			logDetailed("doc:" + doc);
			upres = server.add(doc);
			if (upres == null) {
				throw new KettleException("Unable add document URL :" + url + ": null response");
			}
			logDetailed("ADD:" + upres.getResponse());
			if (upres.getStatus() != 0) {
				throw new KettleException("Unable add document URL :" + url + ":" + upres.getResponse());
			}
			rowCount++;
			if (rowCount == 1000) {
				solrCommit();
			}

		} catch (Exception e) {
			throw new KettleException("Unable to get result from specified URL :" + url, e);
		}

		return new Value("0");
	}

	@SuppressWarnings("deprecation")
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
		meta = (SolrInMeta) smi;
		data = (SolrInData) sdi;

		Object[] r = getRow(); // Get row from input rowset & set row busy!
		if (r == null) // no more input to be expected...
		{
			solrCommit();
			setOutputDone();
			return false;
		}

		try {
			execHttp(r); // add new values to the row
			putRow(getInputRowMeta(), r); // copy row to output rowset(s);

			if (checkFeedback(linesRead))
				logBasic(Messages.getString("SolrIn.LineNumber") + linesRead); //$NON-NLS-1$
		} catch (KettleException e) {
			logError(Messages.getString("SolrIn.ErrorInStepRunning") + e.getMessage()); //$NON-NLS-1$
			setErrors(1);
			stopAll();
			solrCommit();
			setOutputDone(); // signal end to receiver(s)
			return false;
		}

		return true;
	}

	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
		meta = (SolrInMeta) smi;
		data = (SolrInData) sdi;
		url = meta.getUrl();
		server = new HttpSolrClient.Builder(url).withConnectionTimeout(10000).withSocketTimeout(60000).build();
		// server = new HttpSolrServer(url);
		// ((HttpSolrServer) server).setConnectionTimeout(5);
		// ((HttpSolrServer) server).setDefaultMaxConnectionsPerHost(100);
		// ((HttpSolrServer) server).setMaxTotalConnections(100);
		return super.init(smi, sdi);
	}

	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
		meta = (SolrInMeta) smi;
		data = (SolrInData) sdi;

		super.dispose(smi, sdi);
	}

	//
	// Run is were the action happens!
	public void run() {
		logBasic(Messages.getString("SolrIn.Log.StartingToRun")); //$NON-NLS-1$

		try {
			while (processRow(meta, data) && !isStopped())
				;
		} catch (Exception e) {
			logError(Messages.getString("SolrIn.Log.UnexpectedError") + " : " + e.toString()); //$NON-NLS-1$ //$NON-NLS-2$
			logError(Const.getStackTracker(e));
			setErrors(1);
			stopAll();
		} finally {
			dispose(meta, data);
			logSummary();
			markStop();
		}
	}

	private void solrCommit() {
		try {

			if (rowCount > 0) {// 提交solr数据
				logDetailed("commit solr records:" + rowCount);
				UpdateResponse upres = server.commit();
				logDetailed("commit resp:" + upres.getResponse());
				rowCount = 0;
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public String toString() {
		return this.getClass().getName();
	}
}
