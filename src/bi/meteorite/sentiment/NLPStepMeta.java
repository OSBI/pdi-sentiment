/*
* Copyright 2014 OSBI Ltd
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package bi.meteorite.sentiment;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

/**
 * This class is part of the demo step plug-in implementation.
 * It demonstrates the basics of developing a plug-in step for PDI. 
 * 
 * The demo step adds a new string field to the row stream and sets its
 * value to "Hello World!". The user may select the name of the new field.
 *   
 * This class is the implementation of StepMetaInterface.
 * Classes implementing this interface need to:
 * 
 * - keep track of the step settings
 * - serialize step settings both to xml and a repository
 * - provide new instances of objects implementing StepDialogInterface, StepInterface and StepDataInterface
 * - report on how the step modifies the meta-data of the row-stream (row structure and field types)
 * - perform a sanity-check on the settings provided by the user 
 * 
 */

@Step(	
		id = "NLPSentiment",
		image = "bi/meteorite/sentiment/resources/nlp/nlp.jpg",
		i18nPackageName="bi.meteorite.sentiment",
		name="FilemgrCheckStep.Name",
		description = "FilemgrCheckStep.TooltipDesc",
		categoryDescription="i18n:org.pentaho.di.trans.step:BaseStep.Category.Experimental"
)
public class NLPStepMeta extends BaseStepMeta implements StepMetaInterface {
	/**
	 *	The PKG member is used when looking up internationalized strings.
	 *	The properties file with localized keys is expected to reside in 
	 *	{the package of the class specified}/messages/messages_{locale}.properties   
	 */
	private static Class<?> PKG = NLPStepMeta.class; // for i18n purposes
	
	/**
	 * Stores the name of the field added to the row-stream. 
	 */
    private String[] fieldInStream;
    private String[] fieldOutStream;
    private String analysisType;

    /**
	 * Constructor should call super() to make sure the base class has a chance to initialize properly.
	 */
	public NLPStepMeta() {
		super(); 
	}

	/**
	 * Called by PDI to get a new instance of the step data class.
	 */
	/*public StepDataInterface getStepData() {
		return new NLPStepData();
	}*/

    public String[] getFieldInStream() {
        return fieldInStream;
    }

    public void setFieldInStream( String[] keyStream ) {
        this.fieldInStream = keyStream;
    }

    public String[] getFieldOutStream() {
        return fieldOutStream;
    }

    public void setFieldOutStream( String[] keyStream ) {
        this.fieldOutStream = keyStream;
    }

    public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore )
            throws KettleXMLException {
        readData( stepnode );
    }

    public void allocate( int nrkeys ) {
        fieldInStream = new String[nrkeys];
        fieldOutStream = new String[nrkeys];
    }


	/**
	 * This method is used when a step is duplicated in Spoon. It needs to return a deep copy of this
	 * step meta object. Be sure to create proper deep copies if the step configuration is stored in
	 * modifiable objects.
	 * 
	 * See org.pentaho.di.trans.steps.rowgenerator.RowGeneratorMeta.clone() for an example on creating
	 * a deep copy.
	 * 
	 * @return a deep copy of this
	 */
	public Object clone() {
        NLPStepMeta retval = (NLPStepMeta) super.clone();
        int nrkeys = fieldInStream.length;

        retval.allocate( nrkeys );

        for ( int i = 0; i < nrkeys; i++ ) {
            retval.fieldInStream[i] = fieldInStream[i];
            retval.fieldOutStream[i] = fieldOutStream[i];
        }

		return retval;
	}

    private void readData( Node stepnode ) throws KettleXMLException {
        try {
            int nrkeys;

            Node lookup = XMLHandler.getSubNode( stepnode, "fields" );
            nrkeys = XMLHandler.countNodes( lookup, "field" );

            allocate( nrkeys );

            for ( int i = 0; i < nrkeys; i++ ) {
                Node fnode = XMLHandler.getSubNodeByNr(lookup, "field", i);

                fieldInStream[i] = Const.NVL(XMLHandler.getTagValue(fnode, "in_stream_name"), "");
                fieldOutStream[i] = Const.NVL(XMLHandler.getTagValue(fnode, "out_stream_name"), "");

            }
            analysisType = Const.NVL(XMLHandler.getTagValue(stepnode, "analysistype"), "");
        } catch ( Exception e ) {
            throw new KettleXMLException( BaseMessages.getString(
                    PKG, "StringOperationsMeta.Exception.UnableToReadStepInfoFromXML" ), e );
        }
    }

    public void setDefault() {
        fieldInStream = null;
        fieldOutStream = null;
        int nrkeys = 0;

        allocate( nrkeys );
    }
	/**
	 * This method is called by Spoon when a step needs to serialize its configuration to XML. The expected
	 * return value is an XML fragment consisting of one or more XML tags.  
	 * 
	 * Please use org.pentaho.di.core.xml.XMLHandler to conveniently generate the XML.
	 * 
	 * @return a string containing the XML serialization of this step
	 */
    public String getXML() {
        StringBuffer retval = new StringBuffer(500);

        retval.append(" <fields>").append(Const.CR);

        for (int i = 0; i < fieldInStream.length; i++) {
            retval.append(" <field>").append(Const.CR);
            retval.append(" ").append(XMLHandler.addTagValue("in_stream_name", fieldInStream[i]));
            retval.append(" ").append(XMLHandler.addTagValue("out_stream_name", fieldOutStream[i]));
            retval.append( " </field>" ).append( Const.CR );
        }

        retval.append( " </fields>" ).append( Const.CR );
        retval.append(" ").append(XMLHandler.addTagValue("analysistype", analysisType));

        return retval.toString();
    }

    public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
            throws KettleException {
        try {

            int nrkeys = rep.countNrStepAttributes( id_step, "in_stream_name" );

            allocate( nrkeys );
            for ( int i = 0; i < nrkeys; i++ ) {
                fieldInStream[i] = Const.NVL( rep.getStepAttributeString( id_step, i, "in_stream_name" ), "" );
                fieldOutStream[i] = Const.NVL( rep.getStepAttributeString( id_step, i, "out_stream_name" ), "" );
            }
            analysisType = Const.NVL( rep.getStepAttributeString( id_step, "analysistype" ), "" );

        } catch ( Exception e ) {
            throw new KettleException( BaseMessages.getString(
                    PKG, "StringOperationsMeta.Exception.UnexpectedErrorInReadingStepInfo" ), e );
        }
    }

    public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
            throws KettleException {
        try {

            for ( int i = 0; i < fieldInStream.length; i++ ) {
                rep.saveStepAttribute(id_transformation, id_step, i, "in_stream_name", fieldInStream[i]);
                rep.saveStepAttribute(id_transformation, id_step, i, "out_stream_name", fieldOutStream[i]);
            }
            repository.saveStepAttribute(id_transformation, id_step, "analysistype", analysisType);
        } catch ( Exception e ) {
            throw new KettleException( BaseMessages.getString(
                    PKG, "StringOperationsMeta.Exception.UnableToSaveStepInfo" )
                    + id_step, e );
        }
    }

    public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
                           VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {
        // Add new field?
        for ( int i = 0; i < fieldOutStream.length; i++ ) {
            ValueMetaInterface v;
            String outputField = space.environmentSubstitute( fieldOutStream[i] );
            if ( !Const.isEmpty( outputField ) ) {
                // Add a new field
                v = new ValueMeta( outputField, ValueMeta.TYPE_STRING );
                v.setLength( 100, -1 );
                v.setOrigin( name );
                inputRowMeta.addValueMeta( v );
            } else {
                v = inputRowMeta.searchValueMeta( fieldInStream[i] );
                if ( v == null ) {
                    continue;
                }
                v.setStorageType( ValueMetaInterface.STORAGE_TYPE_NORMAL );

            }
        }
    }


    public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepinfo,
                       RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                       Repository repository, IMetaStore metaStore ) {

        CheckResult cr;
        String error_message = "";
        boolean first = true;
        boolean error_found = false;

        if ( prev == null ) {

            error_message +=
                    BaseMessages.getString( PKG, "StringOperationsMeta.CheckResult.NoInputReceived" ) + Const.CR;
            cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepinfo );
            remarks.add( cr );
        } else {

            for ( int i = 0; i < fieldInStream.length; i++ ) {
                String field = fieldInStream[i];

                ValueMetaInterface v = prev.searchValueMeta( field );
                if ( v == null ) {
                    if ( first ) {
                        first = false;
                        error_message +=
                                BaseMessages.getString( PKG, "StringOperationsMeta.CheckResult.MissingInStreamFields" ) + Const.CR;
                    }
                    error_found = true;
                    error_message += "\t\t" + field + Const.CR;
                }
            }
            if ( error_found ) {
                cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepinfo );
            } else {
                cr =
                        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
                                PKG, "StringOperationsMeta.CheckResult.FoundInStreamFields" ), stepinfo );
            }
            remarks.add( cr );

            // Check whether all are strings
            first = true;
            error_found = false;
            for ( int i = 0; i < fieldInStream.length; i++ ) {
                String field = fieldInStream[i];

                ValueMetaInterface v = prev.searchValueMeta( field );
                if ( v != null ) {
                    if ( v.getType() != ValueMeta.TYPE_STRING ) {
                        if ( first ) {
                            first = false;
                            error_message +=
                                    BaseMessages.getString( PKG, "StringOperationsMeta.CheckResult.OperationOnNonStringFields" )
                                            + Const.CR;
                        }
                        error_found = true;
                        error_message += "\t\t" + field + Const.CR;
                    }
                }
            }
            if ( error_found ) {
                cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepinfo );
            } else {
                cr =
                        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
                                PKG, "StringOperationsMeta.CheckResult.AllOperationsOnStringFields" ), stepinfo );
            }
            remarks.add( cr );

            if ( fieldInStream.length > 0 ) {
                for ( int idx = 0; idx < fieldInStream.length; idx++ ) {
                    if ( Const.isEmpty( fieldInStream[idx] ) ) {
                        cr =
                                new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
                                        PKG, "StringOperationsMeta.CheckResult.InStreamFieldMissing", new Integer( idx + 1 )
                                                .toString() ), stepinfo );
                        remarks.add( cr );

                    }
                }
            }

            // Check if all input fields are distinct.
            for ( int idx = 0; idx < fieldInStream.length; idx++ ) {
                for ( int jdx = 0; jdx < fieldInStream.length; jdx++ ) {
                    if ( fieldInStream[idx].equals( fieldInStream[jdx] ) && idx != jdx && idx < jdx ) {
                        error_message =
                                BaseMessages.getString(
                                        PKG, "StringOperationsMeta.CheckResult.FieldInputError", fieldInStream[idx] );
                        cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, error_message, stepinfo );
                        remarks.add( cr );
                    }
                }
            }

        }
    }

    public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
                                  TransMeta transMeta, Trans trans ) {
        return new NLPStep( stepMeta, stepDataInterface, cnr, transMeta, trans );
    }

    public StepDataInterface getStepData() {
        return new NLPStepData();
    }

    public boolean supportsErrorHandling() {
        return true;
    }


    public void setAnalysisType(String analysisType) {
        this.analysisType = analysisType;
    }

    public String getAnalysisType() {
        return analysisType;
    }
}
