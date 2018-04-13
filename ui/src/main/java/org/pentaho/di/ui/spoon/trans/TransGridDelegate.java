/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.ui.spoon.trans;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.DisposeEvent;
import org.eclipse.swt.events.DisposeListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.ToolBar;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.i18n.GlobalMessages;
import org.pentaho.di.trans.step.BaseStepData.StepExecutionStatus;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepStatus;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.XulSpoonSettingsManager;
import org.pentaho.di.ui.spoon.delegates.SpoonDelegate;
import org.pentaho.di.ui.xul.KettleXulLoader;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulLoader;
import org.pentaho.ui.xul.containers.XulToolbar;
import org.pentaho.ui.xul.impl.XulEventHandler;
import org.pentaho.ui.xul.swt.tags.SwtToolbarbutton;

public class TransGridDelegate extends SpoonDelegate implements XulEventHandler {
  private static Class<?> PKG = Spoon.class; // for i18n purposes, needed by Translator2!!

  private static final String XUL_FILE_TRANS_GRID_TOOLBAR = "ui/trans-grid-toolbar.xul";

  public static final long REFRESH_TIME = 100L;

  public static final long UPDATE_TIME_VIEW = 1000L;

  private TransGraph transGraph;

  private CTabItem transGridTab;

  private TableView transGridView;

  private boolean refresh_busy;

  private long lastUpdateView;

  private XulToolbar toolbar;

  private Composite transGridComposite;

  private boolean hideInactiveSteps;

  private boolean showSelectedSteps;

  /**
   * @param spoon
   * @param transGraph
   */
  public TransGridDelegate( Spoon spoon, TransGraph transGraph ) {
    super( spoon );
    this.transGraph = transGraph;

    hideInactiveSteps = false;
  }

  public void showGridView() {

    if ( transGridTab == null || transGridTab.isDisposed() ) {
      addTransGrid();
    } else {
      transGridTab.dispose();

      transGraph.checkEmptyExtraView();
    }
  }

  /**
   * Add a grid with the execution metrics per step in a table view
   *
   */
  public void addTransGrid() {

    // First, see if we need to add the extra view...
    //
    if ( transGraph.extraViewComposite == null || transGraph.extraViewComposite.isDisposed() ) {
      transGraph.addExtraView();
    } else {
      if ( transGridTab != null && !transGridTab.isDisposed() ) {
        // just set this one active and get out...
        //
        transGraph.extraViewTabFolder.setSelection( transGridTab );
        return;
      }
    }

    transGridTab = new CTabItem( transGraph.extraViewTabFolder, SWT.NONE );
    transGridTab.setImage( GUIResource.getInstance().getImageShowGrid() );
    transGridTab.setText( BaseMessages.getString( PKG, "Spoon.TransGraph.GridTab.Name" ) );

    transGridComposite = new Composite( transGraph.extraViewTabFolder, SWT.NONE );
    transGridComposite.setLayout( new FormLayout() );

    addToolBar();

    Control toolbarControl = (Control) toolbar.getManagedObject();

    toolbarControl.setLayoutData( new FormData() );
    FormData fd = new FormData();
    fd.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    toolbarControl.setLayoutData( fd );

    toolbarControl.setParent( transGridComposite );

    //ignore whitespace for stepname column valueMeta, causing sorting to ignore whitespace
    String stepNameColumnName = BaseMessages.getString( PKG, "TransLog.Column.Stepname" );
    ValueMetaInterface valueMeta = new ValueMetaString( stepNameColumnName );
    valueMeta.setIgnoreWhitespace( true );
    ColumnInfo stepNameColumnInfo =
      new ColumnInfo( stepNameColumnName, ColumnInfo.COLUMN_TYPE_TEXT, false,
        true );
    stepNameColumnInfo.setValueMeta( valueMeta );

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        stepNameColumnInfo,
        new ColumnInfo(
          BaseMessages.getString( PKG, "TransLog.Column.Copynr" ), ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "TransLog.Column.Read" ), ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "TransLog.Column.Written" ), ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "TransLog.Column.Input" ), ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "TransLog.Column.Output" ), ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "TransLog.Column.Updated" ), ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "TransLog.Column.Rejected" ), ColumnInfo.COLUMN_TYPE_TEXT, false,
          true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "TransLog.Column.Errors" ), ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "TransLog.Column.Active" ), ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "TransLog.Column.Time" ), ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "TransLog.Column.Speed" ), ColumnInfo.COLUMN_TYPE_TEXT, false, true ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "TransLog.Column.PriorityBufferSizes" ), ColumnInfo.COLUMN_TYPE_TEXT,
          false, true ), };

    colinf[1].setAllignement( SWT.RIGHT );
    colinf[2].setAllignement( SWT.RIGHT );
    colinf[3].setAllignement( SWT.RIGHT );
    colinf[4].setAllignement( SWT.RIGHT );
    colinf[5].setAllignement( SWT.RIGHT );
    colinf[6].setAllignement( SWT.RIGHT );
    colinf[7].setAllignement( SWT.RIGHT );
    colinf[8].setAllignement( SWT.RIGHT );
    colinf[9].setAllignement( SWT.LEFT );
    colinf[10].setAllignement( SWT.RIGHT );
    colinf[11].setAllignement( SWT.RIGHT );
    colinf[12].setAllignement( SWT.RIGHT );

    transGridView = new TableView( transGraph.getManagedObject(), transGridComposite, SWT.BORDER
      | SWT.FULL_SELECTION | SWT.MULTI, colinf, 1,
      true, // readonly!
      null, // Listener
      spoon.props );
    FormData fdView = new FormData();
    fdView.left = new FormAttachment( 0, 0 );
    fdView.right = new FormAttachment( 100, 0 );
    fdView.top = new FormAttachment( (Control) toolbar.getManagedObject(), 0 );
    fdView.bottom = new FormAttachment( 100, 0 );
    transGridView.setLayoutData( fdView );

    // Add a timer to update this view every couple of seconds...
    //
    final Timer tim = new Timer( "TransGraph: " + transGraph.getMeta().getName() );
    final AtomicBoolean busy = new AtomicBoolean( false );

    TimerTask timtask = new TimerTask() {
      public void run() {
        if ( !spoon.getDisplay().isDisposed() ) {
          spoon.getDisplay().asyncExec( new Runnable() {
            public void run() {
              if ( !busy.get() ) {
                busy.set( true );
                refreshView();
                busy.set( false );
              }
            }
          } );
        }
      }
    };

    tim.schedule( timtask, 0L, REFRESH_TIME ); // schedule to repeat a couple of times per second to get fast feedback

    transGridTab.addDisposeListener( new DisposeListener() {
      public void widgetDisposed( DisposeEvent disposeEvent ) {
        tim.cancel();
      }
    } );

    transGridTab.setControl( transGridComposite );

    transGraph.extraViewTabFolder.setSelection( transGridTab );
  }

  private void addToolBar() {

    try {
      XulLoader loader = new KettleXulLoader();
      loader.setSettingsManager( XulSpoonSettingsManager.getInstance() );
      ResourceBundle bundle = GlobalMessages.getBundle( "org/pentaho/di/ui/spoon/messages/messages" );
      XulDomContainer xulDomContainer = loader.loadXul( XUL_FILE_TRANS_GRID_TOOLBAR, bundle );
      xulDomContainer.addEventHandler( this );
      toolbar = (XulToolbar) xulDomContainer.getDocumentRoot().getElementById( "nav-toolbar" );

      ToolBar swtToolBar = (ToolBar) toolbar.getManagedObject();
      spoon.props.setLook( swtToolBar, Props.WIDGET_STYLE_TOOLBAR );
      swtToolBar.layout( true, true );
    } catch ( Throwable t ) {
      log.logError( toString(), Const.getStackTracker( t ) );
      new ErrorDialog( transGridComposite.getShell(),
        BaseMessages.getString( PKG, "Spoon.Exception.ErrorReadingXULFile.Title" ),
        BaseMessages.getString( PKG, "Spoon.Exception.ErrorReadingXULFile.Message", XUL_FILE_TRANS_GRID_TOOLBAR ),
        new Exception( t ) );
    }
  }

  public void showHideInactive() {
    hideInactiveSteps = !hideInactiveSteps;

    SwtToolbarbutton onlyActiveButton = (SwtToolbarbutton) toolbar.getElementById( "show-inactive" );
    if ( onlyActiveButton != null ) {
      onlyActiveButton.setSelected( hideInactiveSteps );
      if ( hideInactiveSteps ) {
        onlyActiveButton.setImage( GUIResource.getInstance().getImageHideInactive() );
      } else {
        onlyActiveButton.setImage( GUIResource.getInstance().getImageShowInactive() );
      }
    }
  }

  public void showHideSelected() {
    showSelectedSteps = !showSelectedSteps;

    SwtToolbarbutton onlySelectedButton = (SwtToolbarbutton) toolbar.getElementById( "show-selected" );
    if ( onlySelectedButton != null ) {
      onlySelectedButton.setSelected( showSelectedSteps );
      if ( showSelectedSteps ) {
        onlySelectedButton.setImage( GUIResource.getInstance().getImageShowSelected() );
      } else {
        onlySelectedButton.setImage( GUIResource.getInstance().getImageShowAll() );
      }
    }
  }

  private void refreshView() {
    boolean insert = true;
    int numberStepsToDisplay = -1;
    int baseStepCount = -1;

    if ( transGridView == null || transGridView.isDisposed() ) {
      return;
    }
    if ( refresh_busy ) {
      return;
    }

    List<StepMeta> selectedSteps = new ArrayList<StepMeta>();
    if ( showSelectedSteps ) {
      selectedSteps = transGraph.trans.getTransMeta().getSelectedSteps();
    }

    int topIdx = transGridView.getTable().getTopIndex();

    refresh_busy = true;

    Table table = transGridView.table;

    long time = new Date().getTime();
    long msSinceLastUpdate = time - lastUpdateView;
    if ( transGraph.trans != null && !transGraph.trans.isPreparing() && msSinceLastUpdate > UPDATE_TIME_VIEW ) {
      lastUpdateView = time;

      baseStepCount = transGraph.trans.nrSteps();


      log.logMinimal( "BaseStepCount = " + baseStepCount );

      StepExecutionStatus[] stepStatusLookup = transGraph.trans.getTransStepExecutionStatusLookup();
      boolean[] isRunningLookup = transGraph.trans.getTransStepIsRunningLookup();

      if ( hideInactiveSteps ) {
        numberStepsToDisplay = transGraph.trans.nrActiveSteps();
        log.logMinimal( "hideInactive - numberStepsToDisplay = " + numberStepsToDisplay );
      } else {
        numberStepsToDisplay = baseStepCount;
      }

      //Count sub steps
      for ( int i = 0; i < baseStepCount; i++ ) {
        //if inactive steps are hidden, only count sub steps of active base steps
        if ( !hideInactiveSteps || (isRunningLookup[i] || stepStatusLookup[i] != StepExecutionStatus.STATUS_FINISHED) ) {
          StepInterface baseStep = transGraph.trans.getRunThread( i );
          numberStepsToDisplay += baseStep.subStatuses().size();
        }
      }
      log.logMinimal( "numberStepsToDisplay = " + numberStepsToDisplay );







      log.logMinimal( "Refresh View3 - Table item count = " + table.getItemCount() );
      if ( table.getItemCount() != numberStepsToDisplay ) {
        table.removeAll();
        log.logMinimal( "Refresh View3 - Table remove all" );
      } else {
        insert = false;
        log.logMinimal( "Refresh View3 - insert = false" );
      }

      if ( numberStepsToDisplay == 0 && table.getItemCount() == 0 ) {
        new TableItem( table, SWT.NONE );
        refresh_busy = false;
        return;
      }

      for ( int i = 0; i < baseStepCount; i++ ) {
        log.logMinimal( "Iterating over total steps i = " + i);
        StepInterface baseStep = transGraph.trans.getRunThread( i );

        // See if the step is selected & in need of display
        //
        boolean showSelected;
        if ( showSelectedSteps ) {
          if ( selectedSteps.size() == 0 ) {
            showSelected = true;
          } else {
            showSelected = false;
            for ( StepMeta stepMeta : selectedSteps ) {
              if ( baseStep.getStepMeta().equals( stepMeta ) ) {
                showSelected = true;
                break;
              }
            }
          }
        } else {
          showSelected = true;
        }

        // when "Hide active" steps is enabled show only alive steps
        // otherwise only those that have not STATUS_EMPTY
        //

        //TODO: HIDING INACTIVE CAUSES SORT TO BREAK, FIGURE OUT HOW TO FIX.

        if ( showSelected
          && ( hideInactiveSteps && ( isRunningLookup[i]
          || stepStatusLookup[i] != StepExecutionStatus.STATUS_FINISHED ) )
          || ( !hideInactiveSteps && stepStatusLookup[i] != StepExecutionStatus.STATUS_EMPTY ) ) {
          TableItem ti = null;
          if ( insert ) {
            log.logMinimal( "New Table Item");
            ti = new TableItem( table, SWT.NONE );
          } else {
            log.logMinimal( "Getting existing item at i = " + i );
            ti = table.getItem( i );
          }


          if ( ti == null ) {
            continue;
          }

          String num = "" + ( i + 1 );
          if ( ti.getText( 0 ).length() < 1 ) {
            ti.setText( 0, num );
          }

          //TODO: I think this is the step that makes it all happen, resets base step to whatever
          //TODO: step number is listed in the table.

          String tableStepNumber = ti.getText( 0 );
          log.logMinimal( "tableStepNumber = " + tableStepNumber );
          String[] tableStepNumberSplit = tableStepNumber.split( "\\." );
          String tableBaseStepNumber = tableStepNumberSplit[0];
          log.logMinimal( "tableBaseStepNumber = " + tableBaseStepNumber );
          String tableSubStepNumber = "0";
          if ( tableStepNumberSplit.length > 1 ) {
             tableSubStepNumber = tableStepNumberSplit[1];
          }
          log.logMinimal( "tableSubStepNumber = " + tableSubStepNumber );





          if ( tableBaseStepNumber.length() > 0 ) {
            //if is a base step number
           // if( tableStepNumber.endsWith( ".0" ) ) {
              Integer tIndex = Integer.parseInt( tableBaseStepNumber );
              tIndex--;

              //replace base step with the base step that matches the table step number
              baseStep = transGraph.trans.getRunThread( tIndex );
           // }

          }

          boolean isSubStep = !"0".equals( tableSubStepNumber );

          if ( !isSubStep ) {
            StepStatus stepStatus = new StepStatus( baseStep );

            String[] fields = stepStatus.getTransLogFields();

            // Anti-flicker: if nothing has changed, don't change it on the
            // screen!
            for ( int f = 1; f < fields.length; f++ ) {
              if ( !fields[ f ].equalsIgnoreCase( ti.getText( f ) ) ) {
                ti.setText( f, fields[ f ] );
              }
            }

            // Error lines should appear in red:
            if ( baseStep.getErrors() > 0 ) {
              ti.setBackground( GUIResource.getInstance().getColorRed() );
            } else {
              ti.setBackground( GUIResource.getInstance().getColorWhite() );
            }

            //write out substeps
            Collection<StepStatus> subStepStatuses = baseStep.subStatuses();
            int subIndex = 1;

            if ( insert ) {
              for ( StepStatus subStepStatus : subStepStatuses ) {
                String[] subFields = subStepStatus.getTransLogFields( baseStep.getStatus().getDescription() );
                subFields[ 1 ] = "     " + subFields[ 1 ];
                TableItem subItem = new TableItem( table, SWT.NONE );
                subItem.setText( 0, num + "." + subIndex++ );
                for ( int f = 1; f < subFields.length; f++ ) {
                  subItem.setText( f, subFields[ f ] );
                }
              }
            }
          } else {

            Collection<StepStatus> subStepStatuses = baseStep.subStatuses();
            int subIndex = 1;
            for ( StepStatus subStepStatus : subStepStatuses ) {
              String[] subFields = subStepStatus.getTransLogFields( baseStep.getStatus().getDescription() );
              if ( subFields[ 0 ].equals( tableSubStepNumber ) ) {
                subFields[ 1 ] = "     " + subFields[ 1 ];
                ti.setText( 0, num + "." + subIndex++ );
                for ( int f = 1; f < subFields.length; f++ ) {
                  ti.setText( f, subFields[ f ] );
                }
              }
            }
          }
        }
      }

      log.logMinimal( "Refresh View4" );
      int sortColumn = transGridView.getSortField();
      boolean sortDescending = transGridView.isSortingDescending();
      // Only need to re-sort if the output has been sorted differently to the default
      if ( table.getItemCount() > 0 && ( sortColumn != 0 || sortDescending ) ) {
        log.logMinimal( "TransGridDelegate - RE-SORT" );
        transGridView.sortTable( transGridView.getSortField(), sortDescending );
      }

      for ( int i = 0; i < table.getItems().length; i++ ) {
        TableItem item = table.getItem( i );
        item.setForeground( GUIResource.getInstance().getColorBlack() );
        if ( !item.getBackground().equals( GUIResource.getInstance().getColorRed() ) ) {
          item.setBackground(
            i % 2 == 0
              ? GUIResource.getInstance().getColorWhite()
              : GUIResource.getInstance().getColorBlueCustomGrid() );
        }
      }

      // if (updateRowNumbers) { transGridView.setRowNums(); }
      transGridView.optWidth( true );

      int[] selectedItems = transGridView.getSelectionIndices();

      if ( selectedItems != null && selectedItems.length > 0 ) {
        transGridView.setSelection( selectedItems );
      }
      // transGridView.getTable().setTopIndex(topIdx);
      if ( transGridView.getTable().getTopIndex() != topIdx ) {
        transGridView.getTable().setTopIndex( topIdx );
      }
    } else {
      // We need at least one table-item in a table!
      if ( table.getItemCount() == 0 ) {
        new TableItem( table, SWT.NONE );
      }
    }

    refresh_busy = false;
  }

  /**
   * Get Row number with value from table
   * @param colnr the column number to search for the value in the table
   * @param cellValue the cell string value to search for in the table
   * @return the first cell in the colnr column containing the cellValue, -1 if the table column row cells do not contain the value
   */
  public int getRowNumberWithValue(Table table, int colnr, String cellValue) {
    int rows = table.getItemCount();
    int column = colnr + 1;
    for ( int i = 0; i < rows; i++ ) {
      TableItem row = table.getItem( i );
      String cell = row.getText( column );
      if(cellValue == null && cell == null) {
        return i;
      }
      if(cellValue.equals( cell )) {
        return i;
      }
    }
    return -1;
  }

  public CTabItem getTransGridTab() {
    return transGridTab;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.ui.xul.impl.XulEventHandler#getData()
   */
  public Object getData() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.ui.xul.impl.XulEventHandler#getName()
   */
  public String getName() {
    return "transgrid";
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.ui.xul.impl.XulEventHandler#getXulDomContainer()
   */
  public XulDomContainer getXulDomContainer() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.ui.xul.impl.XulEventHandler#setData(java.lang.Object)
   */
  public void setData( Object data ) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.ui.xul.impl.XulEventHandler#setName(java.lang.String)
   */
  public void setName( String name ) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   *
   * @see org.pentaho.ui.xul.impl.XulEventHandler#setXulDomContainer(org.pentaho.ui.xul.XulDomContainer)
   */
  public void setXulDomContainer( XulDomContainer xulDomContainer ) {
    // TODO Auto-generated method stub

  }
}
