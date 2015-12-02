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

/********************************************************************
 * A runtime exception that can be thrown from storage-related methods that
 * cannot throw a {@link StorageException}.
 *
 * @author eso
 */
public class StorageRuntimeException extends RuntimeException
{
	//~ Static fields/initializers ---------------------------------------------

	private static final long serialVersionUID = 1L;

	//~ Constructors -----------------------------------------------------------

	/***************************************
	 * @see RuntimeException#RuntimeException()
	 */
	public StorageRuntimeException()
	{
	}

	/***************************************
	 * @see RuntimeException#RuntimeException(String)
	 */
	public StorageRuntimeException(String sMessage)
	{
		super(sMessage);
	}

	/***************************************
	 * @see RuntimeException#RuntimeException(Throwable)
	 */
	public StorageRuntimeException(Throwable eCause)
	{
		super(eCause);
	}

	/***************************************
	 * @see RuntimeException#RuntimeException(String, Throwable)
	 */
	public StorageRuntimeException(String sMessage, Throwable rCause)
	{
		super(sMessage, rCause);
	}
}
