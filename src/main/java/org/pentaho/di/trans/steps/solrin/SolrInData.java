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

import org.pentaho.di.core.database.Database;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;


/**
 * @author Matt
 * @since 24-jan-2005
 *
 */
public class SolrInData extends BaseStepData implements StepDataInterface
{
	public int argnrs[];
	public Database db;

	/**
	 * 
	 */
	public SolrInData()
	{
		super();

		db=null;
	}

}
