//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// This file is a part of the 'esoco-storage' project.
// Copyright 2015 Elmar Sonnenschein, esoco GmbH, Flensburg, Germany
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
package de.esoco.storage;

import java.io.IOException;


/********************************************************************
 * The class for exceptions that are thrown by {@link Storage} implementations.
 *
 * @author eso
 */
public class StorageException extends IOException
{
	//~ Static fields/initializers ---------------------------------------------

	private static final long serialVersionUID = 1L;

	//~ Constructors -----------------------------------------------------------

	/***************************************
	 * @see Exception#Exception()
	 */
	public StorageException()
	{
	}

	/***************************************
	 * @see Exception#Exception(String)
	 */
	public StorageException(String sMessage)
	{
		super(sMessage);
	}

	/***************************************
	 * @see Exception#Exception(Throwable)
	 */
	public StorageException(Throwable eCause)
	{
		super(eCause);
	}

	/***************************************
	 * @see Exception#Exception(String, Throwable)
	 */
	public StorageException(String sMessage, Throwable eCause)
	{
		super(sMessage, eCause);
	}
}
