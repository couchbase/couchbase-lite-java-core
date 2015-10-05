/**
 * Created by Pasin Suriyentrakorn on 8/29/15.
 * <p/>
 * Copyright (c) 2015 Couchbase, Inc All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.couchbase.lite.support.action;

import com.couchbase.lite.Misc;
import com.couchbase.lite.support.FileDirUtils;
import com.couchbase.lite.util.Log;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An AtomicAction implementation to provide an ability to perform actions,
 * back them out if there is an error, and clean up.
 */
public class Action implements AtomicAction {
    private List<ActionBlock> peforms;
    private List<ActionBlock> backouts;
    private List<ActionBlock> cleanUps;

    private int nextStep;

    private Exception lastError;
    private int failedStep;

    private static ActionBlock nullAction;
    static {
        nullAction = new ActionBlock() {
            @Override
            public void execute() throws ActionException { }
        };
    }

    public Action() {
        peforms = new ArrayList<ActionBlock>();
        backouts = new ArrayList<ActionBlock>();
        cleanUps = new ArrayList<ActionBlock>();
        nextStep = 0;
    }

    public Action(ActionBlock perform, ActionBlock backout, ActionBlock cleanup) {
        this();
        add(perform, backout, cleanup);
    }

    /**
     * @return Last exception occurred
     */
    public Exception getLastError() {
        return lastError;
    }

    /**
     * @return Last failure step
     */
    public int getFailedStep() {
        return failedStep;
    }

    /**
     * Adds an action as a step of thid one.
     * @param action
     */
    public void add(final AtomicAction action) {
        if (action instanceof Action) {
            Action a = (Action)action;
            peforms.addAll(a.peforms);
            backouts.addAll(a.backouts);
            cleanUps.addAll(a.cleanUps);
        } else {
            add(new ActionBlock() {
                @Override
                public void execute() throws ActionException {
                    action.perform();
                }
            }, new ActionBlock() {
                @Override
                public void execute() throws ActionException {
                    action.backout();
                }
            }, new ActionBlock() {
                @Override
                public void execute() throws ActionException {
                    action.cleanup();
                }
            });
        }
    }

    /**
     * Adds an action as a step of this one. The action has three components, each optional.
     * @param perform A block that tries to perform the action, or returns an error if it fails.
     *                (If the block fails, it should clean up; the backOut will _not_ be called!)
     * @param backout A block that undoes the effect of the action; it will be called if a _later_
     *                action fails, so that the system can be returned to the initial state.
     * @param cleanup A block that performs any necessary cleanup after all actions have been
     *                performed (e.g. deleting a temporary file.)
     */
    public void add(ActionBlock perform, ActionBlock backout, ActionBlock cleanup) {
        peforms.add(perform != null ? perform : nullAction);
        backouts.add(backout != null ? backout : nullAction);
        cleanUps.add(cleanup != null ? cleanup : nullAction);
    }

    /**
     * Adds an action as a step of this one. The action has two components (backout and cleanup
     * are the same action), each optional.
     * @param perform A block that tries to perform the action, or returns an error if it fails.
     *                (If the block fails, it should clean up; the backOut will _not_ be called!)
     * @param backoutOrCleanup A block that can be both backout and cleanup block.A backout block.
     */
    public void add(ActionBlock perform, ActionBlock backoutOrCleanup) {
        add(perform, backoutOrCleanup, backoutOrCleanup);
    }

    /**
     * Performs all the actions in order.
     * If any action fails, backs out the previously performed actions in reverse order.
     * If the actions succeeded, cleans them up in reverse order.
     * The `lastError` property is set to the exception thrown by the failed perform block.
     * The `failedStep` property is set to the index of the failed perform block.
     * @throws ActionException
     */
    public void run() throws ActionException {
        try {
            perform();
            try {
                cleanup(); // Ignore exception
            } catch (ActionException e) {}
            lastError = null;
        } catch (ActionException e) {
            // (perform: has already backed out whatever it did)
            lastError = e;
            throw e;
        }
    }

    @Override
    public void perform() throws ActionException {
        if (nextStep > 0)
            throw new IllegalStateException("Actions have already been run");

        failedStep = -1;
        for (; nextStep < peforms.size(); ++nextStep) {
            try {
                doAction(peforms);
            } catch (ActionException e) {
                failedStep = nextStep;
                if (nextStep > 0) {
                    try {
                        backout(); // Ignore error
                    } catch (ActionException backoutEx) {}
                }
                throw e;
            }
        }
    }

    @Override
    public void backout() throws ActionException {
        if (nextStep == 0)
            throw new IllegalStateException("Actions have not been run");

        while(nextStep-- > 0) {
            try {
                doAction(backouts);
            } catch (ActionException e) {
                Log.e(Log.TAG_ACTION, "Error backing out step: " + nextStep, e);
                throw e;
            }
        }
    }

    @Override
    public void cleanup() throws ActionException {
        if (nextStep != peforms.size())
            throw new IllegalStateException("Actions did not all run");

        while(nextStep-- > 0) {
            try {
                doAction(cleanUps);
            } catch (ActionException e) {
                Log.e(Log.TAG_ACTION, "Error cleaning up step: " + nextStep, e);
                throw e;
            }
        }
    }

    /**
     * Subroutine that calls an action block from either performs, backOuts or cleanUps.
     * @param actions
     * @throws ActionException
     */
    private void doAction(List<ActionBlock> actions) throws ActionException {
        try {
            actions.get(nextStep).execute();
        } catch (ActionException e) {
            throw e;
        } catch (Exception e) {
            throw new ActionException("Exception raised by step: " + nextStep, e);
        }
    }

    /** File-based actions: **/

    /**
     * Deletes the file/directory at the given path, if it exists.
     * @param path Path to the file or directory to be deleted
     * @param tempDir Path to a tem directory used for backing out the deleted file
     * @return A delete file Action
     */
    public static Action deleteFile(final String path, final String tempDir) {
        if (path == null)
            throw new IllegalArgumentException("The path variable cannot be null");
        if (tempDir == null)
            throw new IllegalArgumentException("The tempDir variable cannot be null");

        final File tempFile = new File(tempDir, Misc.CreateUUID());
        final AtomicBoolean exists = new AtomicBoolean();
        return new Action(
            // Perform:
            new ActionBlock() {
                @Override
                public void execute() throws ActionException {
                    File file = new File(path);
                    if (file.exists()) {
                        if (tempFile == null)
                            throw new ActionException("Cannot perform file deletion as " +
                                    "the temporary file cannot be crated.");

                        boolean success = new File(path).renameTo(tempFile);
                        exists.set(success);
                        if (success)
                            // Mark delete on exit (optional and work on Java only):
                            tempFile.deleteOnExit();
                        else
                            throw new ActionException("Cannot move file " + path +
                                    " to " + tempFile.getAbsolutePath());
                    }
                }
            },
            // Backout:
            new ActionBlock() {
                @Override
                public void execute() throws ActionException {
                    if (exists.get()) {
                        if (!tempFile.renameTo(new File(path)))
                            throw new ActionException("Cannot move file " +
                                    tempFile.getAbsolutePath() + " to " + path);
                    }
                }
            },
            // Cleanup:
            new ActionBlock() {
                @Override
                public void execute() throws ActionException {
                    if (exists.get()) {
                        if (tempFile.isDirectory()) {
                            if (!FileDirUtils.deleteRecursive(tempFile))
                                throw new ActionException("Cannot delete the temporary directory " +
                                        tempFile.getAbsolutePath());
                        } else {
                            if (!tempFile.delete())
                                throw new ActionException("Cannot delete the temporary file " +
                                        tempFile.getAbsolutePath());
                        }
                    }
                }
            });
    }

    /**
     * Moves the file/directory to a new location, which must not already exist.
     * @param srcPath Source path
     * @param destPath Destination path which must not exist
     * @return A moveFileToEmptyPath action
     */
    public static Action moveFileToEmptyPath(final String srcPath, final String destPath) {
        if (srcPath == null)
            throw new IllegalArgumentException("The srcPath variable cannot be null");
        if (destPath == null)
            throw new IllegalArgumentException("The destPath variable cannot be null");

        return new Action(
            // Perform:
            new ActionBlock() {
                @Override
                public void execute() throws ActionException {
                    File destFile = new File(destPath);
                    if (destFile.exists())
                        throw new ActionException("Cannot move file. " +
                                "The Destination file " + destPath + " already exists.");
                    boolean success = new File(srcPath).renameTo(destFile);
                    if (!success)
                        throw new ActionException("Cannot move file " + srcPath +
                                " to " + destPath);
                }
            },
            // Backout:
            new ActionBlock() {
                @Override
                public void execute() throws ActionException {
                    boolean success = new File(destPath).renameTo(new File(srcPath));
                    if (!success)
                        throw new ActionException("Cannot move file " + destPath +
                                " to " + srcPath);
                }
            },
            // Cleanup:
            null);
    }

    /**
     * Moves the file/directory to a new location, replacing anything that already exists there.
     * @param srcPath Source path
     * @param destPath Destination path to be replaced
     * @param tempDir Path to a temp directory used for backing out file deletion
     * @return A moveAndReplaceFile action
     */
    public static Action moveAndReplaceFile(String srcPath, String destPath, String tempDir) {
        Action seq = new Action();
        seq.add(deleteFile(destPath, tempDir));
        seq.add(moveFileToEmptyPath(srcPath, destPath));
        return seq;
    }
}
