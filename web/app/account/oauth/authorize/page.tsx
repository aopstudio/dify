'use client'

import React, { useCallback, useEffect, useMemo, useState } from 'react'
import { useRouter, useSearchParams } from 'next/navigation'
import cn from '@/utils/classnames'
import { type OAuthAppInfo, authorizeOAuthApp, fetchOAuthAppInfo } from '@/service/oauth-provider'

const STORAGE_KEY = 'oauth_authorize_pending'

function buildReturnUrl(pathname: string, search: string) {
  try {
    const base = `${globalThis.location.origin}${pathname}${search}`
    return base
  }
  catch {
    return pathname + search
  }
}

export default function OAuthAuthorizePage() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const client_id = searchParams.get('client_id') || ''
  const redirect_uri = searchParams.get('redirect_uri') || ''
  const state = searchParams.get('state') || ''
  const response_type = searchParams.get('response_type') || 'code'

  const [loading, setLoading] = useState(true)
  const [authorizing, setAuthorizing] = useState(false)
  const [error, setError] = useState<string>('')
  const [appInfo, setAppInfo] = useState<OAuthAppInfo | null>(null)

  const isLoggedIn = useMemo(() => {
    try {
      return Boolean(localStorage.getItem('console_token'))
    }
    catch { return false }
  }, [])

  const init = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      if (!client_id || !redirect_uri || response_type !== 'code') {
        setError('Invalid parameters')
        return
      }
      const info = await fetchOAuthAppInfo(client_id, redirect_uri)
      setAppInfo(info)
    }
    catch (e: any) {
      setError(e?.message || 'Failed to load application info')
    }
    finally {
      setLoading(false)
    }
  }, [client_id, redirect_uri, response_type])

  useEffect(() => {
    init()
  }, [init])

  const onLoginClick = () => {
    try {
      const returnUrl = buildReturnUrl('/account/oauth/authorize', `?client_id=${encodeURIComponent(client_id)}&redirect_uri=${encodeURIComponent(redirect_uri)}${state ? `&state=${encodeURIComponent(state)}` : ''}`)
      localStorage.setItem(STORAGE_KEY, JSON.stringify({ client_id, redirect_uri, state, returnUrl }))
      router.push(`/signin?redirect_url=${encodeURIComponent(returnUrl)}`)
    }
    catch {
      router.push('/signin')
    }
  }

  const onAuthorize = async () => {
    if (!client_id || !redirect_uri)
      return
    setAuthorizing(true)
    setError('')
    try {
      const { code } = await authorizeOAuthApp(client_id)
      const url = new URL(redirect_uri)
      url.searchParams.set('code', code)
      if (state)
        url.searchParams.set('state', state)
      globalThis.location.href = url.toString()
    }
    catch (e: any) {
      setError(e?.message || 'Authorization failed')
      setAuthorizing(false)
    }
  }

  if (loading) {
    return (
      <div className={cn('mx-auto mt-8 w-full px-6 md:px-[108px]')}>
        <div className='system-md-regular text-text-tertiary'>Loading...</div>
      </div>
    )
  }

  if (error) {
    return (
      <div className={cn('mx-auto mt-8 w-full px-6 md:px-[108px]')}>
        <h2 className='title-4xl-semi-bold text-text-primary'>OAuth Authorization</h2>
        <p className='body-md-regular mt-2 text-text-tertiary'>{error}</p>
      </div>
    )
  }

  return (
    <div className={cn('mx-auto mt-8 w-full px-6 md:px-[108px]')}>
      <div className='mx-auto w-full'>
        <h2 className='title-4xl-semi-bold text-text-primary'>OAuth Authorization</h2>
        {!isLoggedIn && (
          <p className='body-md-regular mt-2 text-text-tertiary'>Please login to continue</p>
        )}
      </div>

      {appInfo && (
        <div className='mt-6 rounded-lg bg-components-card-bg p-4 shadow'>
          <div className='flex items-center gap-3'>
            {appInfo.app_icon && (
              <img src={appInfo.app_icon} alt='app icon' className='h-10 w-10 rounded' />
            )}
            <div>
              <div className='title-md-bold text-text-primary'>{appInfo.app_label?.en_US || appInfo.app_label?.zh_Hans || appInfo.app_label?.ja_JP}</div>
              <div className='system-xs-regular break-all text-text-tertiary'>Client ID: {client_id}</div>
            </div>
          </div>
          {appInfo.scope && (
            <div className='mt-4'>
              <div className='system-xs-medium-uppercase mb-2 text-text-tertiary'>Requested Permissions</div>
              <ul className='list-disc pl-6'>
                {appInfo.scope.split(/\s+/).filter(Boolean).map(scope => (
                  <li key={scope} className='body-sm-regular text-text-secondary'>{scope}</li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}

      <div className='mt-6 flex items-center gap-3'>
        {!isLoggedIn ? (
          <button className='btn btn-primary' onClick={onLoginClick}>Login</button>
        ) : (
          <>
            <button className='btn btn-primary' onClick={onAuthorize} disabled={authorizing}>{authorizing ? 'Authorizing...' : 'Authorize'}</button>
            <button className='btn btn-secondary' onClick={() => router.back()}>Cancel</button>
          </>
        )}
      </div>
    </div>
  )
}
