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

import bi.meteorite.sentiment.NLPStepMeta;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;

import java.util.*;
import java.util.List;

/**
 * This class is part of the demo step plug-in implementation.
 * It demonstrates the basics of developing a plug-in step for PDI. 
 * 
 * The demo step adds a new string field to the row stream and sets its
 * value to "Hello World!". The user may select the name of the new field.
 *   
 * This class is the implementation of StepDialogInterface.
 * Classes implementing this interface need to:
 * 
 * - build and open a SWT dialog displaying the step's settings (stored in the step's meta object)
 * - write back any changes the user makes to the step's meta object
 * - report whether the user changed any settings when confirming the dialog 
 * 
 */
public class NLPStepDialog extends BaseStepDialog implements StepDialogInterface {

	/**
	 *	The PKG member is used when looking up internationalized strings.
	 *	The properties file with localized keys is expected to reside in 
	 *	{the package of the class specified}/messages/messages_{locale}.properties   
	 */
	private static Class<?> PKG = NLPStepMeta.class; // for i18n purposes

	// this is the object the stores the step's settings
	// the dialog reads the settings from it when opening
	// the dialog writes the settings to it when confirmed 

	// text field holding the name of the field to add to the row stream
	//private Text wHelloFieldName;

    // text field holding the name of the field to check the filename against
    private Label wlKey;
    private FormData fdlKey, fdKey;
    private TableView wFields;
    private NLPStepMeta input;
    private Map<String, Integer> inputFields;
    private CCombo wAnalysisType;
    private Label wlAnalysisType;
    private FormData fdlAnalysisType,fdAnalysisType;
    private ColumnInfo[] ciKey;
	/**
	 * The constructor should simply invoke super() and save the incoming meta
	 * object to a local variable, so it can conveniently read and write settings
	 * from/to it.
	 * 
	 * @param parent 	the SWT shell to open the dialog in
	 * @param in		the meta object holding the step's settings
	 * @param transMeta	transformation description
	 * @param sname		the step name
	 */
	public NLPStepDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
		super(parent, (BaseStepMeta) in, transMeta, sname);
        input = (NLPStepMeta) in;
        inputFields = new HashMap<String, Integer>();
	}

	/**
	 * This method is called by Spoon when the user opens the settings dialog of the step.
	 * It should open the dialog and return only once the dialog has been closed by the user.
	 * 
	 * If the user confirms the dialog, the meta object (passed in the constructor) must
	 * be updated to reflect the new step settings. The changed flag of the meta object must 
	 * reflect whether the step configuration was changed by the dialog.
	 * 
	 * If the user cancels the dialog, the meta object must not be updated, and its changed flag
	 * must remain unaltered.
	 * 
	 * The open() method must return the name of the step after the user has confirmed the dialog,
	 * or null if the user cancelled the dialog.
	 */
	public String open() {

		// store some convenient SWT variables 
		Shell parent = getParent();
		Display display = parent.getDisplay();

		// SWT code for preparing the dialog
		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
		props.setLook(shell);
		setShellImage(shell, input);
		
		// Save the value of the changed flag on the meta object. If the user cancels
		// the dialog, it will be restored to this saved value.
		// The "changed" variable is inherited from BaseStepDialog
		changed = input.hasChanged();
		
		// The ModifyListener used on all controls. It will update the meta object to 
		// indicate that changes are being made.
		ModifyListener lsMod = new ModifyListener() {
			public void modifyText(ModifyEvent e) {
				input.setChanged();
			}
		};
		
		// ------------------------------------------------------- //
		// SWT code for building the actual settings dialog        //
		// ------------------------------------------------------- //
		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(BaseMessages.getString(PKG, "Demo.Shell.Title")); 

		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		// Stepname line
		wlStepname = new Label(shell, SWT.RIGHT);
		wlStepname.setText(BaseMessages.getString(PKG, "System.Label.StepName")); 
		props.setLook(wlStepname);
		fdlStepname = new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.right = new FormAttachment(middle, -margin);
		fdlStepname.top = new FormAttachment(0, margin);
		wlStepname.setLayoutData(fdlStepname);
		
		wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wStepname.setText(stepname);
		props.setLook(wStepname);
		wStepname.addModifyListener(lsMod);
		fdStepname = new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top = new FormAttachment(0, margin);
		fdStepname.right = new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);

        wlAnalysisType = new Label( shell, SWT.RIGHT );
        wlAnalysisType.setText( BaseMessages.getString( PKG, "Sentiment.Analysis.Label" ) );
        props.setLook( wlAnalysisType );
        fdlAnalysisType = new FormData();
        fdlAnalysisType.left = new FormAttachment( 0, 0 );
        fdlAnalysisType.top = new FormAttachment( wStepname, margin );
        fdlAnalysisType.right = new FormAttachment( middle, -margin );

        wlAnalysisType.setLayoutData( fdlAnalysisType );
        wAnalysisType = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
        props.setLook( wAnalysisType );
        wAnalysisType.addModifyListener( lsMod );
        fdAnalysisType = new FormData();
        fdAnalysisType.left = new FormAttachment( middle, 0 );
        fdAnalysisType.top = new FormAttachment( wStepname, margin );
        fdAnalysisType.right = new FormAttachment( 100, 0 );
        wAnalysisType.setLayoutData( fdAnalysisType );
        wAnalysisType.add( "Wilson Score" );
        wAnalysisType.add( "Simple Vote Rollup" );
        wAnalysisType.add( "Longest Sentence Wins" );
        wAnalysisType.add( "Last Sentence Wins" );
        wAnalysisType.add( "Average Probabilities Rollup" );

        wlKey = new Label( shell, SWT.NONE );
        wlKey.setText( BaseMessages.getString( PKG, "Sentiment.Fields.Label" ) );
        props.setLook( wlKey );
        fdlKey = new FormData();
        fdlKey.left = new FormAttachment( 0, 0 );
        fdlKey.top = new FormAttachment( wAnalysisType, 2 * margin );
        wlKey.setLayoutData( fdlKey );

        int nrFieldCols = 2;
        int nrFieldRows = ( input.getFieldInStream() != null ? input.getFieldInStream().length : 1 );

        ciKey = new ColumnInfo[nrFieldCols];
        ciKey[0] =
                new ColumnInfo(
                        BaseMessages.getString( PKG, "Sentiment.ColumnInfo.InStreamField" ),
                        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
        ciKey[1] =
                new ColumnInfo(
                        BaseMessages.getString( PKG, "Sentiment.ColumnInfo.OutStreamField" ),
                        ColumnInfo.COLUMN_TYPE_TEXT, false );


        ciKey[1]
                .setToolTip( BaseMessages.getString( PKG, "Sentiment.ColumnInfo.OutStreamField.Tooltip" ) );

        wFields =
                new TableView(
                        transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciKey,
                        nrFieldRows, lsMod, props );

        fdKey = new FormData();
        fdKey.left = new FormAttachment( 0, 0 );
        fdKey.top = new FormAttachment( wlKey, margin );
        fdKey.right = new FormAttachment( 100, -margin );
        fdKey.bottom = new FormAttachment( 100, -30 );
        wFields.setLayoutData( fdKey );



        // OK and cancel buttons
		wOK = new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString(PKG, "System.Button.OK")); 
		wCancel = new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

        setButtonPositions(new Button[]{wOK, wCancel}, margin, null);

		// Add listeners for cancel and OK
		lsCancel = new Listener() {
			public void handleEvent(Event e) {cancel();}
		};
		lsOK = new Listener() {
			public void handleEvent(Event e) {ok();}
		};

		wCancel.addListener(SWT.Selection, lsCancel);
		wOK.addListener(SWT.Selection, lsOK);

		// default listener (for hitting "enter")
		lsDef = new SelectionAdapter() {
			public void widgetDefaultSelected(SelectionEvent e) {ok();}
		};
		wStepname.addSelectionListener(lsDef);
		// Detect X or ALT-F4 or something that kills this window and cancel the dialog properly
		shell.addShellListener(new ShellAdapter() {
			public void shellClosed(ShellEvent e) {cancel();}
		});
		
		// Set/Restore the dialog size based on last position on screen
		// The setSize() method is inherited from BaseStepDialog
		setSize();

		// populate the dialog with the values from the meta object
		populateDialog();
		

        final Runnable runnable = new Runnable() {
            public void run() {
                StepMeta stepMeta = transMeta.findStep( stepname );
                if ( stepMeta != null ) {
                    try {
                        RowMetaInterface row = transMeta.getPrevStepFields( stepMeta );
                        if ( row != null ) {
                            // Remember these fields...
                            for ( int i = 0; i < row.size(); i++ ) {
                                inputFields.put( row.getValueMeta( i ).getName(), new Integer( i ) );
                            }

                            setComboBoxes();
                        }

                        // Dislay in red missing field names
                        Display.getDefault().asyncExec( new Runnable() {
                            public void run() {
                                if ( !wFields.isDisposed() ) {
                                    for ( int i = 0; i < wFields.table.getItemCount(); i++ ) {
                                        TableItem it = wFields.table.getItem( i );
                                        if ( !Const.isEmpty( it.getText( 1 ) ) ) {
                                            if ( !inputFields.containsKey( it.getText( 1 ) ) ) {
                                                it.setBackground( GUIResource.getInstance().getColorRed() );
                                            }
                                        }
                                    }
                                }
                            }
                        } );

                    } catch ( KettleException e ) {
                        logError( "Error getting fields from incoming stream!", e );
                    }
                }
            }
        };
        new Thread( runnable ).start();

        input.setChanged( changed );

        shell.open();
        while ( !shell.isDisposed() ) {
            if ( !display.readAndDispatch() ) {
                display.sleep();
            }
        }
        return stepname;
    }

    protected void setComboBoxes() {
        Set<String> keySet = inputFields.keySet();
        List<String> entries = new ArrayList<String>( keySet );
        String[] fieldNames = entries.toArray( new String[entries.size()] );
        Const.sortStrings( fieldNames );
        ciKey[0].setComboValues( fieldNames );

    }
	/**
	 * This helper method puts the step configuration stored in the meta object
	 * and puts it into the dialog controls.
	 */
	private void populateDialog() {
        if ( input.getFieldInStream() != null ) {
            for ( int i = 0; i < input.getFieldInStream().length; i++ ) {
                TableItem item = wFields.table.getItem( i );
                if ( input.getFieldInStream()[i] != null ) {
                    item.setText( 1, input.getFieldInStream()[i] );
                }
                if ( input.getFieldOutStream()[i] != null ) {
                    item.setText( 2, input.getFieldOutStream()[i] );
                }

            }
        }

        if(input.getAnalysisType()!=null) {
            wAnalysisType.setText(input.getAnalysisType());
        }
        else{
            wAnalysisType.setText("Wilson Score");
        }
        wFields.setRowNums();
        wFields.optWidth(true);

        wStepname.selectAll();
        wStepname.setFocus();
	}

	/**
	 * Called when the user cancels the dialog.  
	 */
	private void cancel() {
		// The "stepname" variable will be the return value for the open() method. 
		// Setting to null to indicate that dialog was cancelled.
		stepname = null;
		// Restoring original "changed" flag on the met aobject
		input.setChanged(changed);
		// close the SWT dialog window
		dispose();
	}

    private void getInfo( NLPStepMeta inf ) {
        int nrkeys = wFields.nrNonEmpty();

        inf.allocate( nrkeys );
        if ( isDebug() ) {
            logDebug( BaseMessages.getString( PKG, "StringOperationsDialog.Log.FoundFields", String.valueOf( nrkeys ) ) );
        }
        //CHECKSTYLE:Indentation:OFF
        for ( int i = 0; i < nrkeys; i++ ) {
            TableItem item = wFields.getNonEmpty( i );
            inf.getFieldInStream()[i] = item.getText( 1 );
            inf.getFieldOutStream()[i] = item.getText( 2 );

        }

        inf.setAnalysisType(wAnalysisType.getText());



        stepname = wStepname.getText(); // return value
    }
    private void get() {
        try {
            RowMetaInterface r = transMeta.getPrevStepFields( stepname );
            if ( r != null ) {
                TableItemInsertListener listener = new TableItemInsertListener() {
                    public boolean tableItemInserted( TableItem tableItem, ValueMetaInterface v ) {
                        if ( v.getType() == ValueMeta.TYPE_STRING ) {
                            // Only process strings

                            return true;
                        } else {
                            return false;
                        }
                    }
                };

                BaseStepDialog.getFieldsFromPrevious( r, wFields, 1, new int[] { 1 }, new int[] {}, -1, -1, listener );

            }
        } catch ( KettleException ke ) {
            new ErrorDialog(
                    shell, BaseMessages.getString( PKG, "StringOperationsDialog.FailedToGetFields.DialogTitle" ),
                    BaseMessages.getString( PKG, "StringOperationsDialog.FailedToGetFields.DialogMessage" ), ke );
        }
    }

 	/**
	 * Called when the user confirms the dialog
	 */
	private void ok() {
		// The "stepname" variable will be the return value for the open() method. 
		// Setting to step name from the dialog control
		stepname = wStepname.getText();
        getInfo( input );

		// close the SWT dialog window
		dispose();
	}
}
